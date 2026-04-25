using System;

namespace NetTaskPipeline;

/// <summary>
/// Represents the result of a single task execution.
/// </summary>
public sealed class TaskExecutionResult
{
    /// <summary>
    /// Gets the task display name.
    /// </summary>
    public string TaskName { get; init; } = string.Empty;

    /// <summary>
    /// Gets the index of the group that executed the task.
    /// </summary>
    public int GroupIndex { get; init; }

    /// <summary>
    /// Gets the number of attempts used to execute the task.
    /// </summary>
    public int Attempts { get; init; }

    /// <summary>
    /// Gets the final execution status.
    /// </summary>
    public TaskExecutionStatus Status { get; init; }

    /// <summary>
    /// Gets the exception thrown by the task, if any.
    /// </summary>
    public Exception? Exception { get; init; }

    /// <summary>
    /// Gets the task execution duration.
    /// </summary>
    public TimeSpan Duration { get; init; }

    /// <summary>
    /// Gets the UTC start timestamp.
    /// </summary>
    public DateTimeOffset StartedAt { get; init; }

    /// <summary>
    /// Gets the UTC finish timestamp.
    /// </summary>
    public DateTimeOffset FinishedAt { get; init; }

    /// <summary>
    /// Gets whether the task completed successfully.
    /// </summary>
    public bool Success => Status == TaskExecutionStatus.Success;

    internal static TaskExecutionResult Skipped(string taskName, int groupIndex)
    {
        var now = DateTimeOffset.UtcNow;

        return new TaskExecutionResult
        {
            TaskName = taskName,
            GroupIndex = groupIndex,
            Attempts = 0,
            Status = TaskExecutionStatus.Skipped,
            StartedAt = now,
            FinishedAt = now,
            Duration = TimeSpan.Zero
        };
    }
}
