using System;

namespace NetTaskPipeline;

/// <summary>
/// Represents a failure that occurred while executing a named branch pipeline.
/// </summary>
public sealed class NamedBranchExecutionException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="NamedBranchExecutionException"/> class.
    /// </summary>
    public NamedBranchExecutionException(string branchName, TaskPipelineResult result)
        : base($"The branch '{branchName}' completed with one or more failed tasks.")
    {
        BranchName = branchName;
        Result = result;
    }

    /// <summary>
    /// Gets the selected branch name.
    /// </summary>
    public string BranchName { get; }

    /// <summary>
    /// Gets the branch pipeline result.
    /// </summary>
    public TaskPipelineResult Result { get; }
}
