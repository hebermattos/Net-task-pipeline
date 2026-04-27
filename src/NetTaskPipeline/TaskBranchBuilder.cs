using System;
using System.Collections.Generic;

namespace NetTaskPipeline;

/// <summary>
/// Builds value-based branch flows for <see cref="TaskPipeline"/>.
/// </summary>
public sealed class TaskBranchBuilder<TValue>
{
    private readonly List<TaskBranchCase<TValue>> _cases = new List<TaskBranchCase<TValue>>();
    private Action<TaskPipeline>? _defaultFlow;

    /// <summary>
    /// Adds a branch flow for a specific value.
    /// </summary>
    public TaskBranchBuilder<TValue> When(TValue value, Action<TaskPipeline> flow)
    {
        if (flow == null)
            throw new ArgumentNullException(nameof(flow));

        _cases.Add(new TaskBranchCase<TValue>(value, flow));
        return this;
    }

    /// <summary>
    /// Adds the default branch flow used when no value matches.
    /// </summary>
    public TaskBranchBuilder<TValue> Default(Action<TaskPipeline> flow)
    {
        _defaultFlow = flow ?? throw new ArgumentNullException(nameof(flow));
        return this;
    }

    internal IReadOnlyList<TaskBranchCase<TValue>> Cases => _cases;

    internal Action<TaskPipeline>? DefaultFlow => _defaultFlow;
}

internal sealed class TaskBranchCase<TValue>
{
    public TaskBranchCase(TValue value, Action<TaskPipeline> flow)
    {
        Value = value;
        Flow = flow;
    }

    public TValue Value { get; }

    public Action<TaskPipeline> Flow { get; }
}
