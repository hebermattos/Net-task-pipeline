using Microsoft.Extensions.DependencyInjection;
using NetTaskPipeline;
using Xunit;

namespace NetTaskPipeline.Tests;

public sealed class TaskPipelineServiceProviderExtensionsTests
{
    [Fact]
    public async Task AddTask_WithRegisteredTask_ResolvesTaskFromServiceProvider()
    {
        var services = new ServiceCollection();
        var marker = new ExecutionMarker();

        services.AddSingleton(marker);
        services.AddTransient<RegisteredDependencyTask>();

        using var serviceProvider = services.BuildServiceProvider();

        var result = await new TaskPipeline()
            .AddTask<RegisteredDependencyTask>(serviceProvider)
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.True(marker.WasExecuted);
        Assert.Equal(nameof(RegisteredDependencyTask), Assert.Single(result.TaskResults).TaskName);
    }

    [Fact]
    public async Task AddTask_WithUnregisteredTask_CreatesTaskUsingConstructorDependencies()
    {
        var services = new ServiceCollection();
        var marker = new ExecutionMarker();

        services.AddSingleton(marker);

        using var serviceProvider = services.BuildServiceProvider();

        var result = await new TaskPipeline()
            .AddTask<RegisteredDependencyTask>(serviceProvider)
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.True(marker.WasExecuted);
    }

    [Fact]
    public async Task AddTask_WithCustomName_UsesProvidedTaskName()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new ExecutionMarker());

        using var serviceProvider = services.BuildServiceProvider();

        var result = await new TaskPipeline()
            .AddTask<RegisteredDependencyTask>(serviceProvider, name: "Custom task")
            .ExecuteAsync();

        var taskResult = Assert.Single(result.TaskResults);

        Assert.True(result.Success);
        Assert.Equal("Custom task", taskResult.TaskName);
    }

    [Fact]
    public void AddTask_WithNullPipeline_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();
        using var serviceProvider = services.BuildServiceProvider();

        TaskPipeline pipeline = null!;

        Assert.Throws<ArgumentNullException>(() =>
            pipeline.AddTask<ParameterlessTask>(serviceProvider));
    }

    [Fact]
    public void AddTask_WithNullServiceProvider_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new TaskPipeline().AddTask<ParameterlessTask>((IServiceProvider)null!));
    }

    [Fact]
    public async Task AddParallel_WithServiceProvider_ResolvesBothTasks()
    {
        var services = new ServiceCollection();
        var marker = new ExecutionMarker();

        services.AddSingleton(marker);
        services.AddTransient<FirstParallelDependencyTask>();
        services.AddTransient<SecondParallelDependencyTask>();

        using var serviceProvider = services.BuildServiceProvider();

        var result = await new TaskPipeline()
            .AddParallel<FirstParallelDependencyTask, SecondParallelDependencyTask>(serviceProvider)
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.True(marker.FirstWasExecuted);
        Assert.True(marker.SecondWasExecuted);
        Assert.Equal(2, result.TaskResults.Count);
    }

    [Fact]
    public void AddParallel_WithNullPipeline_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();
        using var serviceProvider = services.BuildServiceProvider();

        TaskPipeline pipeline = null!;

        Assert.Throws<ArgumentNullException>(() =>
            pipeline.AddParallel<ParameterlessTask, ParameterlessTask>(serviceProvider));
    }

    [Fact]
    public void AddParallel_WithNullServiceProvider_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new TaskPipeline().AddParallel<ParameterlessTask, ParameterlessTask>((IServiceProvider)null!));
    }

    [Fact]
    public void AddTaskRpc_WithNullPipeline_ThrowsArgumentNullException()
    {
        TaskPipeline pipeline = null!;

        Assert.Throws<ArgumentNullException>(() =>
            pipeline.AddTaskRpc<RpcRequest, RpcResponse>("Request"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AddTaskRpc_WithInvalidRequestKey_ThrowsArgumentException(string? requestKey)
    {
        Assert.Throws<ArgumentException>(() =>
            new TaskPipeline().AddTaskRpc<RpcRequest, RpcResponse>(requestKey!));
    }

    [Fact]
    public void TaskPipelineResult_WithNullTaskResults_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new TaskPipelineResult(null!, new TaskContext(), TimeSpan.Zero));
    }

    [Fact]
    public void TaskPipelineResult_WithNullContext_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new TaskPipelineResult(Array.Empty<TaskExecutionResult>(), null!, TimeSpan.Zero));
    }

    [Fact]
    public void TaskPipelineResult_Errors_ReturnsOnlyNonSuccessfulResults()
    {
        var failed = new TaskExecutionResult
        {
            TaskName = "Failed",
            Status = TaskExecutionStatus.Failed
        };

        var canceled = new TaskExecutionResult
        {
            TaskName = "Canceled",
            Status = TaskExecutionStatus.Canceled
        };

        var successful = new TaskExecutionResult
        {
            TaskName = "Successful",
            Status = TaskExecutionStatus.Success
        };

        var result = new TaskPipelineResult(
            new[] { failed, canceled, successful },
            new TaskContext(),
            TimeSpan.FromMilliseconds(1));

        Assert.False(result.Success);
        Assert.Equal(new[] { failed, canceled }, result.Errors);
        Assert.Equal(TimeSpan.FromMilliseconds(1), result.Duration);
    }

    private sealed class ExecutionMarker
    {
        public bool WasExecuted { get; set; }

        public bool FirstWasExecuted { get; set; }

        public bool SecondWasExecuted { get; set; }
    }

    private sealed class RegisteredDependencyTask : ITask
    {
        private readonly ExecutionMarker _marker;

        public RegisteredDependencyTask(ExecutionMarker marker)
        {
            _marker = marker;
        }

        public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
        {
            _marker.WasExecuted = true;
            return Task.CompletedTask;
        }
    }

    private sealed class FirstParallelDependencyTask : ITask
    {
        private readonly ExecutionMarker _marker;

        public FirstParallelDependencyTask(ExecutionMarker marker)
        {
            _marker = marker;
        }

        public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
        {
            _marker.FirstWasExecuted = true;
            return Task.CompletedTask;
        }
    }

    private sealed class SecondParallelDependencyTask : ITask
    {
        private readonly ExecutionMarker _marker;

        public SecondParallelDependencyTask(ExecutionMarker marker)
        {
            _marker = marker;
        }

        public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
        {
            _marker.SecondWasExecuted = true;
            return Task.CompletedTask;
        }
    }

    private sealed class ParameterlessTask : ITask
    {
        public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class RpcRequest
    {
        public int Id { get; set; }
    }

    private sealed class RpcResponse
    {
        public int Id { get; set; }
    }
}
