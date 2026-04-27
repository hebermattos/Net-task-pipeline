using NetTaskPipeline;
using Xunit;

namespace NetTaskPipeline.Tests;

public sealed class InlineTaskTests
{
    [Fact]
    public async Task ExecuteAsync_WithNamedInlineTask_ExecutesDelegate()
    {
        var result = await new TaskPipeline()
            .AddTask("Set customer", context =>
            {
                context.Set("CustomerId", 123);
                return Task.CompletedTask;
            })
            .ExecuteAsync();

        var taskResult = Assert.Single(result.TaskResults);

        Assert.True(result.Success);
        Assert.Equal("Set customer", taskResult.TaskName);
        Assert.Equal(123, result.Context.Get<int>("CustomerId"));
    }

    [Fact]
    public async Task ExecuteAsync_WithUnnamedInlineTask_UsesDefaultTaskName()
    {
        var result = await new TaskPipeline()
            .AddTask(context =>
            {
                context.Set("Executed", true);
                return Task.CompletedTask;
            })
            .ExecuteAsync();

        var taskResult = Assert.Single(result.TaskResults);

        Assert.True(result.Success);
        Assert.Equal("InlineTask", taskResult.TaskName);
        Assert.True(result.Context.Get<bool>("Executed"));
    }

    [Fact]
    public async Task ExecuteAsync_WithInlineTaskAndCustomName_UsesCustomTaskName()
    {
        var result = await new TaskPipeline()
            .AddTask(
                context =>
                {
                    context.Set("Executed", true);
                    return Task.CompletedTask;
                },
                name: "Custom inline task")
            .ExecuteAsync();

        var taskResult = Assert.Single(result.TaskResults);

        Assert.True(result.Success);
        Assert.Equal("Custom inline task", taskResult.TaskName);
    }

    [Fact]
    public async Task ExecuteAsync_WithInlineTaskUsingCancellationToken_PassesCancellationToken()
    {
        using var cancellationTokenSource = new CancellationTokenSource();
        var receivedCancellationToken = CancellationToken.None;

        var result = await new TaskPipeline()
            .AddTask("Capture token", (context, cancellationToken) =>
            {
                receivedCancellationToken = cancellationToken;
                context.Set("Executed", true);
                return Task.CompletedTask;
            })
            .ExecuteAsync(cancellationTokenSource.Token);

        Assert.True(result.Success);
        Assert.True(receivedCancellationToken.CanBeCanceled);
        Assert.True(result.Context.Get<bool>("Executed"));
    }

    [Fact]
    public async Task ExecuteAsync_WithInlineTaskRetry_RetriesFailedDelegate()
    {
        var attempts = 0;

        var result = await new TaskPipeline()
            .AddTask("Unstable inline task", _ =>
            {
                attempts++;

                if (attempts < 2)
                    throw new InvalidOperationException("Temporary failure.");

                return Task.CompletedTask;
            }, retryCount: 1)
            .ExecuteAsync();

        var taskResult = Assert.Single(result.TaskResults);

        Assert.True(result.Success);
        Assert.Equal(2, attempts);
        Assert.Equal(2, taskResult.Attempts);
    }

    [Fact]
    public async Task ExecuteAsync_WithInlineTaskTimeout_ReturnsFailedResult()
    {
        var result = await new TaskPipeline()
            .AddTask(
                "Slow inline task",
                async (_, cancellationToken) => await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken),
                timeout: TimeSpan.FromMilliseconds(100))
            .ExecuteAsync();

        var taskResult = Assert.Single(result.TaskResults);

        Assert.False(result.Success);
        Assert.Equal("Slow inline task", taskResult.TaskName);
        Assert.Equal(TaskExecutionStatus.Failed, taskResult.Status);
        Assert.IsType<TimeoutException>(taskResult.Exception);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AddTask_WithInvalidInlineTaskName_ThrowsArgumentException(string? name)
    {
        Assert.Throws<ArgumentException>(() =>
            new TaskPipeline().AddTask(name!, _ => Task.CompletedTask));
    }

    [Fact]
    public void AddTask_WithNullInlineDelegate_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new TaskPipeline().AddTask("Invalid inline task", (Func<TaskContext, Task>)null!));
    }
}
