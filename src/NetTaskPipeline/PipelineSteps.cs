using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace NetTaskPipeline;

internal sealed class PipelineTask
{
    internal PipelineTask(ITask task, string name, int? retryCount, TimeSpan? timeout)
    {
        Task = task;
        Name = name;
        RetryCount = retryCount;
        Timeout = timeout;
    }

    internal ITask Task { get; }
    internal string Name { get; }
    internal int? RetryCount { get; }
    internal TimeSpan? Timeout { get; }
}

internal interface IPipelineStep
{
    Task<IReadOnlyList<TaskExecutionResult>> ExecuteAsync(TaskPipeline pipeline, TaskContext context, int groupIndex, CancellationToken cancellationToken);
}

internal sealed class TaskGroupStep : IPipelineStep
{
    internal TaskGroupStep(TaskGroup group) => Group = group;
    private TaskGroup Group { get; }

    public Task<IReadOnlyList<TaskExecutionResult>> ExecuteAsync(TaskPipeline pipeline, TaskContext context, int groupIndex, CancellationToken cancellationToken)
        => pipeline.ExecuteTaskGroupStepAsync(Group, context, groupIndex, cancellationToken);
}

internal sealed class ValueBranchStep<TValue> : IPipelineStep
{
    internal ValueBranchStep(string name, Func<TaskContext, CancellationToken, Task<TValue>> selector, IReadOnlyList<ValueBranchCase<TValue>> cases, TaskPipeline? defaultPipeline)
    {
        Name = name;
        Selector = selector;
        Cases = cases;
        DefaultPipeline = defaultPipeline;
    }

    private string Name { get; }
    private Func<TaskContext, CancellationToken, Task<TValue>> Selector { get; }
    private IReadOnlyList<ValueBranchCase<TValue>> Cases { get; }
    private TaskPipeline? DefaultPipeline { get; }

    public async Task<IReadOnlyList<TaskExecutionResult>> ExecuteAsync(TaskPipeline pipeline, TaskContext context, int groupIndex, CancellationToken cancellationToken)
    {
        TValue selectedValue;
        try
        {
            selectedValue = await Selector(context, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new[] { TaskPipeline.CreateBranchFailureResult(Name, groupIndex, ex) };
        }

        var selectedPipeline = Cases.FirstOrDefault(branchCase => EqualityComparer<TValue>.Default.Equals(branchCase.Value, selectedValue))?.Pipeline
            ?? DefaultPipeline;

        if (selectedPipeline == null)
            return new[] { TaskPipeline.CreateBranchFailureResult(Name, groupIndex, new InvalidOperationException($"No branch matched the selected value '{selectedValue}' and no default branch was configured.")) };

        var branchResult = await selectedPipeline.ExecuteAsync(context, cancellationToken).ConfigureAwait(false);
        return branchResult.TaskResults;
    }
}

internal sealed class ValueBranchCase<TValue>
{
    internal ValueBranchCase(TValue value, TaskPipeline pipeline)
    {
        Value = value;
        Pipeline = pipeline;
    }

    internal TValue Value { get; }
    internal TaskPipeline Pipeline { get; }
}

internal sealed class TaskGroup
{
    private TaskGroup(bool isParallel, IReadOnlyList<PipelineTask> tasks)
    {
        IsParallel = isParallel;
        Tasks = tasks;
    }

    internal bool IsParallel { get; }
    internal IReadOnlyList<PipelineTask> Tasks { get; }
    internal static TaskGroup Sequential(PipelineTask task) => new TaskGroup(false, new[] { task });
    internal static TaskGroup Parallel(IReadOnlyList<PipelineTask> tasks) => new TaskGroup(true, tasks);
}
