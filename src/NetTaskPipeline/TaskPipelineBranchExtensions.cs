using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NetTaskPipeline;

/// <summary>
/// Provides branching extensions for <see cref="TaskPipeline"/>.
/// </summary>
public static class TaskPipelineBranchExtensions
{
    /// <summary>
    /// Adds a named branch step using a fluent branch builder.
    /// </summary>
    public static TaskPipeline AddBranch(
        this TaskPipeline pipeline,
        Func<TaskContext, string> branchNameSelector,
        Action<NamedBranchBuilder> configure,
        string? name = null)
    {
        if (branchNameSelector == null)
            throw new ArgumentNullException(nameof(branchNameSelector));

        return pipeline.AddBranch(
            (context, _) => Task.FromResult(branchNameSelector(context)),
            configure,
            name);
    }

    /// <summary>
    /// Adds an asynchronous named branch step using a fluent branch builder.
    /// </summary>
    public static TaskPipeline AddBranch(
        this TaskPipeline pipeline,
        Func<TaskContext, CancellationToken, Task<string>> branchNameSelector,
        Action<NamedBranchBuilder> configure,
        string? name = null)
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        if (branchNameSelector == null)
            throw new ArgumentNullException(nameof(branchNameSelector));

        if (configure == null)
            throw new ArgumentNullException(nameof(configure));

        var builder = new NamedBranchBuilder();
        configure(builder);

        return pipeline.AddNamedBranch(
            branchNameSelector,
            builder.Branches,
            builder.DefaultBranch,
            name);
    }

    /// <summary>
    /// Adds a named branch step. The selected branch name is resolved from the shared context.
    /// </summary>
    public static TaskPipeline AddNamedBranch(
        this TaskPipeline pipeline,
        Func<TaskContext, string> branchNameSelector,
        IDictionary<string, Action<TaskPipeline>> branches,
        Action<TaskPipeline>? defaultBranch = null,
        string? name = null)
    {
        if (branchNameSelector == null)
            throw new ArgumentNullException(nameof(branchNameSelector));

        return pipeline.AddNamedBranch(
            (context, _) => Task.FromResult(branchNameSelector(context)),
            branches,
            defaultBranch,
            name);
    }

    /// <summary>
    /// Adds an asynchronous named branch step. The selected branch name is resolved from the shared context.
    /// </summary>
    public static TaskPipeline AddNamedBranch(
        this TaskPipeline pipeline,
        Func<TaskContext, CancellationToken, Task<string>> branchNameSelector,
        IDictionary<string, Action<TaskPipeline>> branches,
        Action<TaskPipeline>? defaultBranch = null,
        string? name = null)
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        if (branchNameSelector == null)
            throw new ArgumentNullException(nameof(branchNameSelector));

        if (branches == null)
            throw new ArgumentNullException(nameof(branches));

        if (branches.Count == 0 && defaultBranch == null)
            throw new ArgumentException("At least one branch or a default branch must be configured.", nameof(branches));

        var branchPipelines = new Dictionary<string, TaskPipeline>(StringComparer.OrdinalIgnoreCase);

        foreach (var branch in branches)
        {
            if (string.IsNullOrWhiteSpace(branch.Key))
                throw new ArgumentException("Branch names cannot be null, empty, or whitespace.", nameof(branches));

            if (branch.Value == null)
                throw new ArgumentException($"Branch '{branch.Key}' does not have a configuration action.", nameof(branches));

            if (branchPipelines.ContainsKey(branch.Key))
                throw new ArgumentException($"A branch named '{branch.Key}' was already configured.", nameof(branches));

            var childPipeline = new TaskPipeline();
            branch.Value(childPipeline);

            branchPipelines.Add(branch.Key, childPipeline);
        }

        TaskPipeline? defaultPipeline = null;

        if (defaultBranch != null)
        {
            defaultPipeline = new TaskPipeline();
            defaultBranch(defaultPipeline);
        }

        return pipeline.AddTask(
            new NamedBranchTask(
                name ?? "Named branch",
                branchNameSelector,
                branchPipelines,
                defaultPipeline),
            name: name ?? "Named branch");
    }
}
