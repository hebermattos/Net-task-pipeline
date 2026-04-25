using NetTaskPipeline;
using Xunit;

namespace NetTaskPipeline.Tests;

public sealed class FluentBranchTests
{
    [Fact]
    public async Task ExecuteAsync_WithFluentBranch_ExecutesSelectedBranch()
    {
        var context = new TaskContext();
        context.Set("Flow", "premium");

        var result = await new TaskPipeline()
            .AddBranch(
                ctx => ctx.Get<string>("Flow"),
                branch => branch
                    .When("premium", new SetContextTask("SelectedFlow", "premium"))
                    .When("standard", new SetContextTask("SelectedFlow", "standard"))
                    .When("blocked", new SetContextTask("SelectedFlow", "blocked")),
                name: "Flow decision")
            .ExecuteAsync(context);

        Assert.True(result.Success);
        Assert.Equal("premium", result.Context.Get<string>("SelectedFlow"));
    }

    [Fact]
    public async Task ExecuteAsync_WithFluentBranchDefault_ExecutesDefaultBranch()
    {
        var context = new TaskContext();
        context.Set("Flow", "unknown");

        var result = await new TaskPipeline()
            .AddBranch(
                ctx => ctx.Get<string>("Flow"),
                branch => branch
                    .When("premium", new SetContextTask("SelectedFlow", "premium"))
                    .Default(new SetContextTask("SelectedFlow", "default")),
                name: "Flow decision")
            .ExecuteAsync(context);

        Assert.True(result.Success);
        Assert.Equal("default", result.Context.Get<string>("SelectedFlow"));
    }

    [Fact]
    public async Task ExecuteAsync_WithFluentBranchPipeline_ExecutesFullSubPipeline()
    {
        var context = new TaskContext();
        context.Set("Flow", "premium");

        var result = await new TaskPipeline()
            .AddBranch(
                ctx => ctx.Get<string>("Flow"),
                branch => branch
                    .When("premium", pipeline => pipeline
                        .AddTask(new SetContextTask("Discount", "20%"))
                        .AddTask(new SetContextTask("EmailTemplate", "premium-template")))
                    .When("standard", pipeline => pipeline
                        .AddTask(new SetContextTask("Discount", "5%"))
                        .AddTask(new SetContextTask("EmailTemplate", "standard-template"))),
                name: "Flow decision")
            .ExecuteAsync(context);

        Assert.True(result.Success);
        Assert.Equal("20%", result.Context.Get<string>("Discount"));
        Assert.Equal("premium-template", result.Context.Get<string>("EmailTemplate"));
    }

    private sealed class SetContextTask : ITask
    {
        private readonly string _key;
        private readonly string _value;

        public SetContextTask(string key, string value)
        {
            _key = key;
            _value = value;
        }

        public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
        {
            context.Set(_key, _value);
            return Task.CompletedTask;
        }
    }
}
