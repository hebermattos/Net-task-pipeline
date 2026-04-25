using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NetTaskPipeline;

internal sealed class NamedBranchTask : ITask
{
    private readonly string _name;
    private readonly Func<TaskContext, CancellationToken, Task<string>> _branchNameSelector;
    private readonly IReadOnlyDictionary<string, TaskPipeline> _branches;
    private readonly TaskPipeline? _defaultBranch;

    public NamedBranchTask(
        string name,
        Func<TaskContext, CancellationToken, Task<string>> branchNameSelector,
        IReadOnlyDictionary<string, TaskPipeline> branches,
        TaskPipeline? defaultBranch)
    {
        _name = name;
        _branchNameSelector = branchNameSelector;
        _branches = branches;
        _defaultBranch = defaultBranch;
    }

    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        var branchName = await _branchNameSelector(context, cancellationToken).ConfigureAwait(false);

        if (string.IsNullOrWhiteSpace(branchName))
            throw new InvalidOperationException($"The branch selector for '{_name}' returned an empty branch name.");

        if (!_branches.TryGetValue(branchName, out var selectedPipeline))
            selectedPipeline = _defaultBranch;

        if (selectedPipeline == null)
            throw new InvalidOperationException($"No branch named '{branchName}' was configured for '{_name}', and no default branch is available.");

        var result = await selectedPipeline.ExecuteAsync(context, cancellationToken).ConfigureAwait(false);

        if (!result.Success)
            throw new NamedBranchExecutionException(branchName, result);
    }
}
