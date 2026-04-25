namespace NetTaskPipeline;

/// <summary>
/// Defines how the pipeline behaves when a task fails.
/// </summary>
public enum ErrorMode
{
    /// <summary>
    /// Stops the pipeline execution after the first failed, canceled, or skipped task.
    /// </summary>
    StopOnFirstError,

    /// <summary>
    /// Continues executing the remaining task groups even if a task fails.
    /// </summary>
    ContinueOnError
}
