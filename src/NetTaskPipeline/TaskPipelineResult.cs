using System;
using System.Collections.Generic;
using System.Linq;

namespace NetTaskPipeline;

/// <summary>
/// Represents the result of a full pipeline execution.
/// </summary>
public sealed class TaskPipelineResult
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TaskPipelineResult"/> class.
    /// </summary>
    public TaskPipelineResult(
        IReadOnlyList<TaskExecutionResult> taskResults,
        TaskContext context,
        TimeSpan duration)
    {
        TaskResults = taskResults ?? throw new ArgumentNullException(nameof(taskResults));
        Context = context ?? throw new ArgumentNullException(nameof(context));
        Duration = duration;
    }

    /// <summary>
    /// Gets the result of each executed task.
    /// </summary>
    public IReadOnlyList<TaskExecutionResult> TaskResults { get; }

    /// <summary>
    /// Gets the shared context used during execution.
    /// </summary>
    public TaskContext Context { get; }

    /// <summary>
    /// Gets the total pipeline duration.
    /// </summary>
    public TimeSpan Duration { get; }

    /// <summary>
    /// Gets whether all tasks completed successfully.
    /// </summary>
    public bool Success => TaskResults.All(result => result.Success);

    /// <summary>
    /// Gets all non-successful task results.
    /// </summary>
    public IReadOnlyList<TaskExecutionResult> Errors =>
        TaskResults
            .Where(result => !result.Success)
            .ToList();
}
