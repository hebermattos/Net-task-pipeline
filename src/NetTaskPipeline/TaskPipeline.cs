using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace NetTaskPipeline;

/// <summary>Executes tasks in sequential groups, allowing each group to run one or more tasks in parallel.</summary>
public sealed class TaskPipeline
{
    private static readonly Random JitterRandom = new Random();
    private static readonly object JitterLock = new object();
    private readonly List<IPipelineStep> _steps = new List<IPipelineStep>();
    private Func<Type, ITask>? _taskFactory;
    private ErrorMode _errorMode = ErrorMode.StopOnFirstError;
    private int _defaultRetryCount;
    private Func<int, TimeSpan>? _retryDelay;
    private Func<Exception, bool>? _shouldRetry;
    private TimeSpan? _defaultTimeout;
    private int? _maxDegreeOfParallelism;

    public TaskPipeline()
    {
    }

    public TaskPipeline OnError(ErrorMode errorMode)
    {
        _errorMode = errorMode;
        return this;
    }

    public TaskPipeline WithRetry(int retryCount)
    {
        if (retryCount < 0)
            throw new ArgumentOutOfRangeException(nameof(retryCount), "Retry count cannot be negative.");

        _defaultRetryCount = retryCount;
        return this;
    }

    public TaskPipeline WithRetryDelay(TimeSpan delay, bool exponentialBackoff = false, bool jitter = false)
    {
        if (delay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(delay), "Retry delay cannot be negative.");

        _retryDelay = attempt =>
        {
            var multiplier = exponentialBackoff ? Math.Pow(2, Math.Min(attempt - 1, 20)) : 1d;
            var ticks = Math.Min(delay.Ticks * multiplier, TimeSpan.MaxValue.Ticks);
            if (jitter && ticks > 0)
            {
                double jitterFactor;
                lock (JitterLock)
                    jitterFactor = 0.5d + JitterRandom.NextDouble() * 0.5d;

                ticks *= jitterFactor;
            }

            return TimeSpan.FromTicks((long)ticks);
        };
        return this;
    }

    public TaskPipeline WithRetryPolicy(Func<Exception, bool> shouldRetry)
    {
        _shouldRetry = shouldRetry ?? throw new ArgumentNullException(nameof(shouldRetry));
        return this;
    }

    public TaskPipeline WithTimeout(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be greater than zero.");

        _defaultTimeout = timeout;
        return this;
    }

    public TaskPipeline WithMaxDegreeOfParallelism(int maxDegreeOfParallelism)
    {
        if (maxDegreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism), "The maximum degree of parallelism must be greater than zero.");

        _maxDegreeOfParallelism = maxDegreeOfParallelism;
        return this;
    }

    internal TaskPipeline RegisterTaskFactory(Func<Type, ITask> taskFactory)
    {
        _taskFactory = taskFactory ?? throw new ArgumentNullException(nameof(taskFactory));
        return this;
    }

    internal TTask CreateTask<TTask>() where TTask : ITask
    {
        return _taskFactory != null
            ? (TTask)_taskFactory(typeof(TTask))
            : Activator.CreateInstance<TTask>();
    }

    internal TaskPipeline AddTask(ITask task, int? retryCount = null, TimeSpan? timeout = null, string? name = null)
    {
        if (task == null)
            throw new ArgumentNullException(nameof(task));

        _steps.Add(new TaskGroupStep(TaskGroup.Sequential(new PipelineTask(task, name ?? task.GetType().Name, retryCount, timeout))));
        return this;
    }

    internal TaskPipeline AddTask(params ITask[] tasks) => AddParallel(tasks);

    internal TaskPipeline AddParallel(IEnumerable<ITask> tasks, int? retryCount = null, TimeSpan? timeout = null)
    {
        if (tasks == null)
            throw new ArgumentNullException(nameof(tasks));

        var pipelineTasks = tasks.Select(task =>
        {
            if (task == null)
                throw new ArgumentException("The task list cannot contain null items.", nameof(tasks));

            return new PipelineTask(task, task.GetType().Name, retryCount, timeout);
        }).ToList();

        if (pipelineTasks.Count == 0)
            return this;

        _steps.Add(new TaskGroupStep(pipelineTasks.Count == 1
            ? TaskGroup.Sequential(pipelineTasks[0])
            : TaskGroup.Parallel(pipelineTasks)));

        return this;
    }

    public TaskPipeline AddBranch<TValue>(Func<TaskContext, TValue> selector, Action<TaskBranchBuilder<TValue>> configure, string? name = null)
    {
        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        return AddBranch((context, _) => Task.FromResult(selector(context)), configure, name);
    }

    public TaskPipeline AddBranch<TValue>(Func<TaskContext, CancellationToken, Task<TValue>> selector, Action<TaskBranchBuilder<TValue>> configure, string? name = null)
    {
        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        if (configure == null)
            throw new ArgumentNullException(nameof(configure));

        var builder = new TaskBranchBuilder<TValue>();
        configure(builder);

        var branches = builder.Cases
            .Select(branchCase => new ValueBranchCase<TValue>(
                branchCase.Value,
                CreateConfiguredChildPipeline(branchCase.Flow)))
            .ToList();

        var defaultPipeline = builder.DefaultFlow == null
            ? null
            : CreateConfiguredChildPipeline(builder.DefaultFlow);

        _steps.Add(new ValueBranchStep<TValue>(
            name ?? "Value branch",
            selector,
            branches,
            defaultPipeline));

        return this;
    }

    public Task<TaskPipelineResult> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        return ExecuteAsync(new TaskContext(), cancellationToken);
    }

    public async Task<TaskPipelineResult> ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        var pipelineStopwatch = Stopwatch.StartNew();
        var allResults = new List<TaskExecutionResult>();

        for (var stepIndex = 0; stepIndex < _steps.Count; stepIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var step = _steps[stepIndex];
            var stepResults = await step.ExecuteAsync(this, context, stepIndex, cancellationToken).ConfigureAwait(false);

            allResults.AddRange(stepResults);

            if (_errorMode == ErrorMode.StopOnFirstError && stepResults.Any(result => !result.Success))
                break;
        }

        pipelineStopwatch.Stop();
        return new TaskPipelineResult(allResults, context, pipelineStopwatch.Elapsed);
    }

    private TaskPipeline CreateChildPipeline()
    {
        return new TaskPipeline
        {
            _errorMode = _errorMode,
            _defaultRetryCount = _defaultRetryCount,
            _retryDelay = _retryDelay,
            _shouldRetry = _shouldRetry,
            _defaultTimeout = _defaultTimeout,
            _maxDegreeOfParallelism = _maxDegreeOfParallelism,
            _taskFactory = _taskFactory
        };
    }

    private TaskPipeline CreateConfiguredChildPipeline(Action<TaskPipeline> configure)
    {
        var pipeline = CreateChildPipeline();
        configure(pipeline);
        return pipeline;
    }

    private async Task<IReadOnlyList<TaskExecutionResult>> ExecuteTaskGroupStepAsync(TaskGroup group, TaskContext context, int groupIndex, CancellationToken cancellationToken)
    {
        if (group.IsParallel)
            return await ExecuteParallelGroupAsync(group, context, groupIndex, cancellationToken).ConfigureAwait(false);

        var pipelineTask = group.Tasks[0];
        var result = await TaskExecutionEngine.ExecuteAsync(
            pipelineTask.Task,
            pipelineTask.Name,
            groupIndex,
            CreateExecutionOptions(pipelineTask),
            context,
            cancellationToken,
            cancellationToken).ConfigureAwait(false);
        return new[] { result };
    }

    private async Task<IReadOnlyList<TaskExecutionResult>> ExecuteParallelGroupAsync(TaskGroup group, TaskContext context, int groupIndex, CancellationToken cancellationToken)
    {
        using var groupCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var maxDegreeOfParallelism = Math.Min(_maxDegreeOfParallelism ?? group.Tasks.Count, group.Tasks.Count);
        return await ParallelTaskExecutor.ExecuteAsync(
            group.Tasks,
            maxDegreeOfParallelism,
            async (pipelineTask, _) =>
            {
                if (groupCancellationTokenSource.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                    return TaskExecutionResult.Skipped(pipelineTask.Name, groupIndex);

                var result = await TaskExecutionEngine.ExecuteAsync(
                    pipelineTask.Task,
                    pipelineTask.Name,
                    groupIndex,
                    CreateExecutionOptions(pipelineTask),
                    context,
                    cancellationToken,
                    groupCancellationTokenSource.Token).ConfigureAwait(false);

                if (_errorMode == ErrorMode.StopOnFirstError && !result.Success)
                    groupCancellationTokenSource.Cancel();

                return result;
            },
            cancellationToken).ConfigureAwait(false);
    }

    private TaskExecutionOptions CreateExecutionOptions(PipelineTask task) => new TaskExecutionOptions
    {
        RetryCount = task.RetryCount ?? _defaultRetryCount,
        Timeout = task.Timeout ?? _defaultTimeout,
        RetryDelay = _retryDelay,
        ShouldRetry = _shouldRetry
    };

    private static TaskExecutionResult CreateBranchFailureResult(string branchName, int groupIndex, Exception exception)
    {
        var now = DateTimeOffset.UtcNow;
        return new TaskExecutionResult
        {
            TaskName = branchName,
            GroupIndex = groupIndex,
            Attempts = 1,
            Status = TaskExecutionStatus.Failed,
            Exception = exception,
            Duration = TimeSpan.Zero,
            StartedAt = now,
            FinishedAt = now
        };
    }

    private sealed class PipelineTask
    {
        public PipelineTask(ITask task, string name, int? retryCount, TimeSpan? timeout)
        {
            Task = task;
            Name = name;
            RetryCount = retryCount;
            Timeout = timeout;
        }

        public ITask Task { get; }
        public string Name { get; }
        public int? RetryCount { get; }
        public TimeSpan? Timeout { get; }
    }

    private interface IPipelineStep
    {
        Task<IReadOnlyList<TaskExecutionResult>> ExecuteAsync(TaskPipeline pipeline, TaskContext context, int groupIndex, CancellationToken cancellationToken);
    }

    private sealed class TaskGroupStep : IPipelineStep
    {
        public TaskGroupStep(TaskGroup group)
        {
            Group = group;
        }

        private TaskGroup Group { get; }

        public Task<IReadOnlyList<TaskExecutionResult>> ExecuteAsync(TaskPipeline pipeline, TaskContext context, int groupIndex, CancellationToken cancellationToken)
        {
            return pipeline.ExecuteTaskGroupStepAsync(Group, context, groupIndex, cancellationToken);
        }
    }

    private sealed class ValueBranchStep<TValue> : IPipelineStep
    {
        public ValueBranchStep(string name, Func<TaskContext, CancellationToken, Task<TValue>> selector, IReadOnlyList<ValueBranchCase<TValue>> cases, TaskPipeline? defaultPipeline)
        {
            Name = name;
            Selector = selector;
            Cases = cases;
            DefaultPipeline = defaultPipeline;
        }

        public string Name { get; }
        public Func<TaskContext, CancellationToken, Task<TValue>> Selector { get; }
        public IReadOnlyList<ValueBranchCase<TValue>> Cases { get; }
        public TaskPipeline? DefaultPipeline { get; }

        public async Task<IReadOnlyList<TaskExecutionResult>> ExecuteAsync(TaskPipeline pipeline, TaskContext context, int groupIndex, CancellationToken cancellationToken)
        {
            TValue selectedValue;
            try
            {
                selectedValue = await Selector(context, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                return new[] { CreateBranchFailureResult(Name, groupIndex, ex) };
            }

            var selectedPipeline = Cases.FirstOrDefault(branchCase => EqualityComparer<TValue>.Default.Equals(branchCase.Value, selectedValue))?.Pipeline
                ?? DefaultPipeline;

            if (selectedPipeline == null)
                return Array.Empty<TaskExecutionResult>();

            var branchResult = await selectedPipeline.ExecuteAsync(context, cancellationToken).ConfigureAwait(false);
            return branchResult.TaskResults;
        }
    }

    private sealed class ValueBranchCase<TValue>
    {
        public ValueBranchCase(TValue value, TaskPipeline pipeline)
        {
            Value = value;
            Pipeline = pipeline;
        }

        public TValue Value { get; }
        public TaskPipeline Pipeline { get; }
    }

    private sealed class TaskGroup
    {
        private TaskGroup(bool isParallel, IReadOnlyList<PipelineTask> tasks)
        {
            IsParallel = isParallel;
            Tasks = tasks;
        }

        public bool IsParallel { get; }
        public IReadOnlyList<PipelineTask> Tasks { get; }
        public static TaskGroup Sequential(PipelineTask task) => new TaskGroup(false, new[] { task });
        public static TaskGroup Parallel(IReadOnlyList<PipelineTask> tasks) => new TaskGroup(true, tasks);
    }
}
