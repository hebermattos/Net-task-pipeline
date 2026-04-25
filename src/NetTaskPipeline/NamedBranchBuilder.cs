using System;
using System.Collections.Generic;

namespace NetTaskPipeline;

/// <summary>
/// Provides a fluent API to configure named branch pipelines.
/// </summary>
public sealed class NamedBranchBuilder
{
    private readonly Dictionary<string, Action<TaskPipeline>> _branches = new Dictionary<string, Action<TaskPipeline>>(StringComparer.OrdinalIgnoreCase);

    private Action<TaskPipeline>? _defaultBranch;

    internal IDictionary<string, Action<TaskPipeline>> Branches => _branches;

    internal Action<TaskPipeline>? DefaultBranch => _defaultBranch;

    /// <summary>
    /// Adds a named branch using a full pipeline configuration.
    /// </summary>
    public NamedBranchBuilder When(string branchName, Action<TaskPipeline> configure)
    {
        if (string.IsNullOrWhiteSpace(branchName))
            throw new ArgumentException("Branch name cannot be null, empty, or whitespace.", nameof(branchName));

        if (configure == null)
            throw new ArgumentNullException(nameof(configure));

        if (_branches.ContainsKey(branchName))
            throw new ArgumentException($"A branch named '{branchName}' was already configured.", nameof(branchName));

        _branches.Add(branchName, configure);

        return this;
    }

    /// <summary>
    /// Adds a named branch using a task type.
    /// </summary>
    public NamedBranchBuilder When<TTask>(string branchName)
        where TTask : ITask, new()
    {
        return When(branchName, pipeline => pipeline.AddTask<TTask>());
    }

    /// <summary>
    /// Adds a named branch using two task types executed in parallel.
    /// </summary>
    public NamedBranchBuilder When<TTask1, TTask2>(string branchName)
        where TTask1 : ITask, new()
        where TTask2 : ITask, new()
    {
        return When(branchName, pipeline => pipeline.AddParallel<TTask1, TTask2>());
    }

    /// <summary>
    /// Adds a named branch using three task types executed in parallel.
    /// </summary>
    public NamedBranchBuilder When<TTask1, TTask2, TTask3>(string branchName)
        where TTask1 : ITask, new()
        where TTask2 : ITask, new()
        where TTask3 : ITask, new()
    {
        return When(branchName, pipeline => pipeline.AddParallel<TTask1, TTask2, TTask3>());
    }

    internal NamedBranchBuilder When(string branchName, params ITask[] tasks)
    {
        if (tasks == null)
            throw new ArgumentNullException(nameof(tasks));

        return When(branchName, pipeline => pipeline.AddTask(tasks));
    }

    /// <summary>
    /// Adds the default branch used when no configured branch name matches the selected branch name.
    /// </summary>
    public NamedBranchBuilder Default(Action<TaskPipeline> configure)
    {
        if (configure == null)
            throw new ArgumentNullException(nameof(configure));

        _defaultBranch = configure;

        return this;
    }

    /// <summary>
    /// Adds the default branch using a task type.
    /// </summary>
    public NamedBranchBuilder Default<TTask>()
        where TTask : ITask, new()
    {
        return Default(pipeline => pipeline.AddTask<TTask>());
    }

    /// <summary>
    /// Adds the default branch using two task types executed in parallel.
    /// </summary>
    public NamedBranchBuilder Default<TTask1, TTask2>()
        where TTask1 : ITask, new()
        where TTask2 : ITask, new()
    {
        return Default(pipeline => pipeline.AddParallel<TTask1, TTask2>());
    }

    internal NamedBranchBuilder Default(params ITask[] tasks)
    {
        if (tasks == null)
            throw new ArgumentNullException(nameof(tasks));

        return Default(pipeline => pipeline.AddTask(tasks));
    }
}
