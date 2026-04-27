using NetTaskPipeline;
using Xunit;

namespace NetTaskPipeline.Tests;

public sealed class TaskBranchBuilderTests
{
    [Fact]
    public async Task AddBranch_WithWhenAction_ExecutesMatchingFlow()
    {
        var context = new TaskContext();
        context.Set("CustomerType", "premium");

        var result = await new TaskPipeline()
            .AddBranch(
                ctx => ctx.Get<string>("CustomerType"),
                branch => branch
                    .When("premium", flow => flow.AddTask(new DelegateTask("Premium", ctx =>
                    {
                        ctx.Set("SelectedFlow", "premium");
                        return Task.CompletedTask;
                    })))
                    .When("standard", flow => flow.AddTask(new DelegateTask("Standard", ctx =>
                    {
                        ctx.Set("SelectedFlow", "standard");
                        return Task.CompletedTask;
                    })))
                    .Default(flow => flow.AddTask(new DelegateTask("Default", ctx =>
                    {
                        ctx.Set("SelectedFlow", "default");
                        return Task.CompletedTask;
                    }))),
                name: "Customer type")
            .ExecuteAsync(context);

        var taskResult = Assert.Single(result.TaskResults);

        Assert.True(result.Success);
        Assert.Equal("premium", result.Context.Get<string>("SelectedFlow"));
        Assert.Equal("DelegateTask", taskResult.TaskName);
    }

    [Fact]
    public async Task AddBranch_WithDefaultAction_ExecutesDefaultFlowWhenNoValueMatches()
    {
        var context = new TaskContext();
        context.Set("CustomerType", "unknown");

        var result = await new TaskPipeline()
            .AddBranch(
                ctx => ctx.Get<string>("CustomerType"),
                branch => branch
                    .When("premium", flow => flow.AddTask(new DelegateTask("Premium", _ => Task.CompletedTask)))
                    .Default(flow => flow.AddTask(new DelegateTask("Default", ctx =>
                    {
                        ctx.Set("SelectedFlow", "default");
                        return Task.CompletedTask;
                    }))),
                name: "Customer type")
            .ExecuteAsync(context);

        var taskResult = Assert.Single(result.TaskResults);

        Assert.True(result.Success);
        Assert.Equal("default", result.Context.Get<string>("SelectedFlow"));
        Assert.Equal("DelegateTask", taskResult.TaskName);
    }

    [Fact]
    public async Task AddBranch_WithNoMatchingValueAndNoDefault_ReturnsNoTaskResults()
    {
        var context = new TaskContext();
        context.Set("CustomerType", "unknown");

        var result = await new TaskPipeline()
            .AddBranch(
                ctx => ctx.Get<string>("CustomerType"),
                branch => branch.When("premium", flow => flow.AddTask(new DelegateTask("Premium", _ => Task.CompletedTask))),
                name: "Customer type")
            .ExecuteAsync(context);

        Assert.True(result.Success);
        Assert.Empty(result.TaskResults);
    }

    [Fact]
    public async Task AddBranch_WithAsyncSelector_ExecutesMatchingFlow()
    {
        var context = new TaskContext();
        context.Set("OrderTotal", 1500m);

        var result = await new TaskPipeline()
            .AddBranch(
                async (ctx, cancellationToken) =>
                {
                    await Task.Delay(1, cancellationToken);
                    return ctx.Get<decimal>("OrderTotal") >= 1000m ? "high" : "low";
                },
                branch => branch
                    .When("high", flow => flow.AddTask(new DelegateTask("High", ctx =>
                    {
                        ctx.Set("Approval", "required");
                        return Task.CompletedTask;
                    })))
                    .When("low", flow => flow.AddTask(new DelegateTask("Low", ctx =>
                    {
                        ctx.Set("Approval", "automatic");
                        return Task.CompletedTask;
                    }))),
                name: "Approval")
            .ExecuteAsync(context);

        var taskResult = Assert.Single(result.TaskResults);

        Assert.True(result.Success);
        Assert.Equal("required", result.Context.Get<string>("Approval"));
        Assert.Equal("DelegateTask", taskResult.TaskName);
    }

    [Fact]
    public async Task AddBranch_WithSelectorFailure_ReturnsFailedResult()
    {
        var result = await new TaskPipeline()
            .AddBranch<string>(
                _ => throw new InvalidOperationException("Selector failed."),
                branch => branch.When("premium", flow => flow.AddTask(new DelegateTask("Premium", _ => Task.CompletedTask))),
                name: "Failing selector")
            .ExecuteAsync();

        var taskResult = Assert.Single(result.TaskResults);

        Assert.False(result.Success);
        Assert.Equal("Failing selector", taskResult.TaskName);
        Assert.Equal(TaskExecutionStatus.Failed, taskResult.Status);
        Assert.IsType<InvalidOperationException>(taskResult.Exception);
    }

    [Fact]
    public void AddBranch_WithNullSelector_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new TaskPipeline().AddBranch(
                (Func<TaskContext, string>)null!,
                branch => branch.When("premium", flow => flow.AddTask(new DelegateTask("Premium", _ => Task.CompletedTask)))));
    }

    [Fact]
    public void AddBranch_WithNullConfigure_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new TaskPipeline().AddBranch(
                (Func<TaskContext, string>)(ctx => ctx.Get<string>("CustomerType")),
                (Action<TaskBranchBuilder<string>>)null!));
    }

    [Fact]
    public void When_WithNullAction_ThrowsArgumentNullException()
    {
        var builder = new TaskBranchBuilder<string>();

        Assert.Throws<ArgumentNullException>(() => builder.When("premium", null!));
    }

    [Fact]
    public void Default_WithNullAction_ThrowsArgumentNullException()
    {
        var builder = new TaskBranchBuilder<string>();

        Assert.Throws<ArgumentNullException>(() => builder.Default(null!));
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
