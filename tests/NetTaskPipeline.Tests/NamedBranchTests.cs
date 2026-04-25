using NetTaskPipeline;
using Xunit;

namespace NetTaskPipeline.Tests;

public sealed class NamedBranchTests
{
    [Fact]
    public async Task ExecuteAsync_WithNamedBranch_ExecutesSelectedBranch()
    {
        var context = new TaskContext();
        context.Set("CustomerType", "premium");

        var result = await new TaskPipeline()
            .AddNamedBranch(
                branchNameSelector: ctx => ctx.Get<string>("CustomerType"),
                branches: new Dictionary<string, Action<TaskPipeline>>
                {
                    ["premium"] = branch => branch.AddTask(new DelegateTask("Premium flow", ctx =>
                    {
                        ctx.Set("SelectedFlow", "premium");
                        return Task.CompletedTask;
                    })),
                    ["standard"] = branch => branch.AddTask(new DelegateTask("Standard flow", ctx =>
                    {
                        ctx.Set("SelectedFlow", "standard");
                        return Task.CompletedTask;
                    })),
                    ["blocked"] = branch => branch.AddTask(new DelegateTask("Blocked flow", ctx =>
                    {
                        ctx.Set("SelectedFlow", "blocked");
                        return Task.CompletedTask;
                    }))
                },
                name: "Customer type decision")
            .ExecuteAsync(context);

        Assert.True(result.Success);
        Assert.Equal("premium", result.Context.Get<string>("SelectedFlow"));
        Assert.Contains(result.TaskResults, taskResult => taskResult.TaskName == "Customer type decision");
    }

    [Fact]
    public async Task ExecuteAsync_WithNamedBranch_ExecutesDefaultBranchWhenNameIsNotConfigured()
    {
        var context = new TaskContext();
        context.Set("CustomerType", "unknown");

        var result = await new TaskPipeline()
            .AddNamedBranch(
                branchNameSelector: ctx => ctx.Get<string>("CustomerType"),
                branches: new Dictionary<string, Action<TaskPipeline>>
                {
                    ["premium"] = branch => branch.AddTask(new DelegateTask("Premium flow", ctx =>
                    {
                        ctx.Set("SelectedFlow", "premium");
                        return Task.CompletedTask;
                    }))
                },
                defaultBranch: branch => branch.AddTask(new DelegateTask("Default flow", ctx =>
                {
                    ctx.Set("SelectedFlow", "default");
                    return Task.CompletedTask;
                })),
                name: "Customer type decision")
            .ExecuteAsync(context);

        Assert.True(result.Success);
        Assert.Equal("default", result.Context.Get<string>("SelectedFlow"));
    }

    [Fact]
    public async Task ExecuteAsync_WithNamedBranchWithoutDefault_ReturnsFailedResultWhenNameIsNotConfigured()
    {
        var context = new TaskContext();
        context.Set("CustomerType", "unknown");

        var result = await new TaskPipeline()
            .AddNamedBranch(
                branchNameSelector: ctx => ctx.Get<string>("CustomerType"),
                branches: new Dictionary<string, Action<TaskPipeline>>
                {
                    ["premium"] = branch => branch.AddTask(new DelegateTask("Premium flow", _ => Task.CompletedTask))
                },
                name: "Customer type decision")
            .ExecuteAsync(context);

        var taskResult = Assert.Single(result.TaskResults);

        Assert.False(result.Success);
        Assert.Equal("Customer type decision", taskResult.TaskName);
        Assert.Equal(TaskExecutionStatus.Failed, taskResult.Status);
        Assert.IsType<InvalidOperationException>(taskResult.Exception);
    }

    [Fact]
    public async Task ExecuteAsync_WithAsyncNamedBranch_ExecutesSelectedBranch()
    {
        var context = new TaskContext();
        context.Set("Total", 1500m);

        var result = await new TaskPipeline()
            .AddNamedBranch(
                branchNameSelector: async (ctx, cancellationToken) =>
                {
                    await Task.Delay(50, cancellationToken);
                    return ctx.Get<decimal>("Total") >= 1000m ? "high-value" : "low-value";
                },
                branches: new Dictionary<string, Action<TaskPipeline>>
                {
                    ["high-value"] = branch => branch.AddTask(new DelegateTask("High value flow", ctx =>
                    {
                        ctx.Set("RequiresApproval", true);
                        return Task.CompletedTask;
                    })),
                    ["low-value"] = branch => branch.AddTask(new DelegateTask("Low value flow", ctx =>
                    {
                        ctx.Set("RequiresApproval", false);
                        return Task.CompletedTask;
                    }))
                },
                name: "Approval decision")
            .ExecuteAsync(context);

        Assert.True(result.Success);
        Assert.True(result.Context.Get<bool>("RequiresApproval"));
    }

    [Fact]
    public async Task ExecuteAsync_WithNamedBranchThatFails_ReturnsFailedResult()
    {
        var context = new TaskContext();
        context.Set("Flow", "failure");

        var result = await new TaskPipeline()
            .AddNamedBranch(
                branchNameSelector: ctx => ctx.Get<string>("Flow"),
                branches: new Dictionary<string, Action<TaskPipeline>>
                {
                    ["failure"] = branch => branch.AddTask(new DelegateTask("Failing branch task", _ =>
                    {
                        throw new InvalidOperationException("Branch task failed.");
                    }))
                },
                name: "Failure decision")
            .ExecuteAsync(context);

        var taskResult = Assert.Single(result.TaskResults);

        Assert.False(result.Success);
        Assert.Equal("Failure decision", taskResult.TaskName);
        Assert.Equal(TaskExecutionStatus.Failed, taskResult.Status);
        Assert.IsType<NamedBranchExecutionException>(taskResult.Exception);
    }

    private sealed class DelegateTask : ITask
    {
        private readonly Func<TaskContext, Task> _execute;

        public DelegateTask(string name, Func<TaskContext, Task> execute)
        {
            Name = name;
            _execute = execute;
        }

        public string Name { get; }

        public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
        {
            return _execute(context);
        }
    }
}
