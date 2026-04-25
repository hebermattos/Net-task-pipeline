namespace NetTaskPipeline;

/// <summary>
/// Represents the final status of a task execution.
/// </summary>
public enum TaskExecutionStatus
{
    /// <summary>
    /// The task completed successfully.
    /// </summary>
    Success,

    /// <summary>
    /// The task failed with an exception.
    /// </summary>
    Failed,

    /// <summary>
    /// The task was canceled.
    /// </summary>
    Canceled,

    /// <summary>
    /// The task was not executed because the pipeline stopped earlier.
    /// </summary>
    Skipped
}
