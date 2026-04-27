using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace NetTaskPipeline;

/// <summary>Executes tasks in sequential groups, allowing each group to run one or more tasks in parallel.</summary>
public sealed class TaskPipeline
{
    private readonly List<PipelineStep> _steps = new List<PipelineStep>();
    private IServiceProvider? _serviceProvider;
    private ErrorMode _errorMode = ErrorMode.StopOnFirstError;
    private int _defaultRetryCount;
    private TimeSpan? _defaultTimeout;
    private int? _maxDegreeOfParallelism;

    public TaskPipeline()
    {
    }

    public TaskPipeline(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
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

    internal TTask CreateTask<TTask>() where TTask : ITask
    {
        return _serviceProvider != null
            ? ActivatorUtilities.GetServiceOrCreateInstance<TTask>(_serviceProvider)
            : Activator.CreateInstance<TTask>();
    }

    internal TaskPipeline AddTask(ITask task, int? retryCount = null, TimeSpan? timeout = null, string? name = null)
    {
        if (task == null)
            throw new ArgumentNullException(nameof(task));

        _steps.Add(PipelineStep.TaskGroup(TaskGroup.Sequential(new PipelineTask(task, name ?? task.GetType().Name, retryCount, timeout))));
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

        _steps.Add(PipelineStep.TaskGroup(pipelineTasks.Count == 1
            ? TaskGroup.Sequential(pipelineTasks[0])
            : TaskGroup.Parallel(pipelineTasks)));

        return this;
    }

    public TaskPipeline AddBranch(Func<TaskContext, bool> condition, Action<TaskPipeline> whenTrue, Action<TaskPipeline>? whenFalse = null, string? name = null)
    {
        if (condition == null)
            throw new ArgumentNullException(nameof(condition));

        return AddBranch((context, _) => Task.FromResult(condition(context)), whenTrue, whenFalse, name);
    }

    public TaskPipeline AddBranch(Func<TaskContext, CancellationToken, Task<bool>> condition, Action<TaskPipeline> whenTrue, Action<TaskPipeline>? whenFalse = null, string? name = null)
    {
        if (condition == null)
            throw new ArgumentNullException(nameof(condition));

        if (whenTrue == null)
            throw new ArgumentNullException(nameof(whenTrue));

        var truePipeline = CreateChildPipeline();
        whenTrue(truePipeline);

        TaskPipeline? falsePipeline = null;
        if (whenFalse != null)
        {
            falsePipeline = CreateChildPipeline();
            whenFalse(falsePipeline);
        }

        _steps.Add(PipelineStep.Branch(new BranchStep(name ?? "Conditional branch", condition, truePipeline, falsePipeline)));
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

        _steps.Add(PipelineStep.ValueBranch(new ValueBranchStep<TValue>(
            name ?? "Value branch",
            selector,
            branches,
            defaultPipeline)));

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
            IReadOnlyList<TaskExecutionResult> stepResults = step.TaskGroupValue != null
                ? await ExecuteTaskGroupStepAsync(step.TaskGroupValue, context, stepIndex, cancellationToken).ConfigureAwait(false)
                : step.BranchValue != null
                    ? await ExecuteBranchStepAsync(step.BranchValue, context, stepIndex, cancellationToken).ConfigureAwait(false)
                    : step.ValueBranchValue != null
                        ? await step.ValueBranchValue.ExecuteAsync(context, stepIndex, cancellationToken).ConfigureAwait(false)
                        : Array.Empty<TaskExecutionResult>();

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
            _defaultTimeout = _defaultTimeout,
            _maxDegreeOfParallelism = _maxDegreeOfParallelism,
            _serviceProvider = _serviceProvider
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

        var result = await ExecuteTaskAsync(group.Tasks[0], context, groupIndex, cancellationToken, cancellationToken).ConfigureAwait(false);
        return new[] { result };
    }

    private async Task<IReadOnlyList<TaskExecutionResult>> ExecuteBranchStepAsync(BranchStep branch, TaskContext context, int groupIndex, CancellationToken cancellationToken)
    {
        bool conditionResult;
        try
        {
            conditionResult = await branch.Condition(context, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new[] { CreateBranchFailureResult(branch.Name, groupIndex, ex) };
        }

        var selectedPipeline = conditionResult ? branch.WhenTrue : branch.WhenFalse;
        if (selectedPipeline == null)
            return Array.Empty<TaskExecutionResult>();

        var branchResult = await selectedPipeline.ExecuteAsync(context, cancellationToken).ConfigureAwait(false);
        return branchResult.TaskResults;
    }

    private async Task<IReadOnlyList<TaskExecutionResult>> ExecuteParallelGroupAsync(TaskGroup group, TaskContext context, int groupIndex, CancellationToken cancellationToken)
    {
        using var groupCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var maxDegreeOfParallelism = Math.Min(_maxDegreeOfParallelism ?? group.Tasks.Count, group.Tasks.Count);
        using var semaphore = new SemaphoreSlim(maxDegreeOfParallelism);

        var executions = group.Tasks.Select(async pipelineTask =>
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (groupCancellationTokenSource.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                    return TaskExecutionResult.Skipped(pipelineTask.Name, groupIndex);

                var result = await ExecuteTaskAsync(pipelineTask, context, groupIndex, cancellationToken, groupCancellationTokenSource.Token).ConfigureAwait(false);

                if (_errorMode == ErrorMode.StopOnFirstError && !result.Success)
                    groupCancellationTokenSource.Cancel();

                return result;
            }
            finally
            {
                semaphore.Release();
            }
        });

        return await Task.WhenAll(executions).ConfigureAwait(false);
    }

    private async Task<TaskExecutionResult> ExecuteTaskAsync(PipelineTask pipelineTask, TaskContext context, int groupIndex, CancellationToken rootCancellationToken, CancellationToken executionCancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var startedAt = DateTimeOffset.UtcNow;
        var retryCount = pipelineTask.RetryCount ?? _defaultRetryCount;
        var timeout = pipelineTask.Timeout ?? _defaultTimeout;
        var maxAttempts = retryCount + 1;
        Exception? lastException = null;
        var status = TaskExecutionStatus.Failed;
        var attempts = 0;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            attempts = attempt;
            rootCancellationToken.ThrowIfCancellationRequested();

            using var timeoutCancellationTokenSource = CreateTimeoutCancellationTokenSource(executionCancellationToken, timeout);
            try
            {
                await pipelineTask.Task.ExecuteAsync(context, timeoutCancellationTokenSource.Token).ConfigureAwait(false);
                status = TaskExecutionStatus.Success;
                lastException = null;
                break;
            }
            catch (OperationCanceledException ex) when (!rootCancellationToken.IsCancellationRequested)
            {
                if (timeoutCancellationTokenSource.IsCancellationRequested && !executionCancellationToken.IsCancellationRequested)
                {
                    lastException = new TimeoutException($"The task '{pipelineTask.Name}' exceeded the configured timeout of {timeout}.", ex);
                    status = TaskExecutionStatus.Failed;
                }
                else
                {
                    lastException = ex;
                    status = TaskExecutionStatus.Canceled;
                    break;
                }
            }
            catch (Exception ex)
            {
                lastException = ex;
                status = TaskExecutionStatus.Failed;
            }
        }

        stopwatch.Stop();
        return new TaskExecutionResult
        {
            TaskName = pipelineTask.Name,
            GroupIndex = groupIndex,
            Attempts = attempts,
            Status = status,
            Exception = lastException,
            Duration = stopwatch.Elapsed,
            StartedAt = startedAt,
            FinishedAt = DateTimeOffset.UtcNow
        };
    }

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

    private static CancellationTokenSource CreateTimeoutCancellationTokenSource(CancellationToken cancellationToken, TimeSpan? timeout)
    {
        var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (timeout.HasValue)
            cancellationTokenSource.CancelAfter(timeout.Value);

        return cancellationTokenSource;
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

    private sealed class BranchStep
    {
        public BranchStep(string name, Func<TaskContext, CancellationToken, Task<bool>> condition, TaskPipeline whenTrue, TaskPipeline? whenFalse)
        {
            Name = name;
            Condition = condition;
            WhenTrue = whenTrue;
            WhenFalse = whenFalse;
        }

        public string Name { get; }
        public Func<TaskContext, CancellationToken, Task<bool>> Condition { get; }
        public TaskPipeline WhenTrue { get; }
        public TaskPipeline? WhenFalse { get; }
    }

    private interface IValueBranchStep
    {
        Task<IReadOnlyList<TaskExecutionResult>> ExecuteAsync(TaskContext context, int groupIndex, CancellationToken cancellationToken);
    }

    private sealed class ValueBranchStep<TValue> : IValueBranchStep
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

        public async Task<IReadOnlyList<TaskExecutionResult>> ExecuteAsync(TaskContext context, int groupIndex, CancellationToken cancellationToken)
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

    private sealed class PipelineStep
    {
        private PipelineStep(TaskGroup? taskGroup, BranchStep? branch, IValueBranchStep? valueBranch)
        {
            TaskGroupValue = taskGroup;
            BranchValue = branch;
            ValueBranchValue = valueBranch;
        }

        public TaskGroup? TaskGroupValue { get; }
        public BranchStep? BranchValue { get; }
        public IValueBranchStep? ValueBranchValue { get; }
        public static PipelineStep TaskGroup(TaskGroup taskGroup) => new PipelineStep(taskGroup, null, null);
        public static PipelineStep Branch(BranchStep branch) => new PipelineStep(null, branch, null);
        public static PipelineStep ValueBranch<TValue>(ValueBranchStep<TValue> branch) => new PipelineStep(null, null, branch);
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
