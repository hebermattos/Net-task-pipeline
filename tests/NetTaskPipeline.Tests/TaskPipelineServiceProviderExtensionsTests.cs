using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using NetTaskPipeline;
using Xunit;

namespace NetTaskPipeline.Tests;

public sealed class TaskPipelineServiceProviderExtensionsTests
{
    [Fact]
    public async Task WithServiceProvider_ResolvesTaskFromPipelineServiceProvider()
    {
        var services = new ServiceCollection();
        var marker = new ExecutionMarker();

        services.AddSingleton(marker);
        services.AddTransient<RegisteredDependencyTask>();

        using var serviceProvider = services.BuildServiceProvider();

        var result = await new TaskPipeline()
            .WithServiceProvider(serviceProvider)
            .AddTask<RegisteredDependencyTask>()
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.True(marker.WasExecuted);
        Assert.Equal(nameof(RegisteredDependencyTask), Assert.Single(result.TaskResults).TaskName);
    }

    [Fact]
    public async Task WithServiceProvider_CanReplacePreviousServiceProvider()
    {
        var firstServices = new ServiceCollection();
        var firstMarker = new ExecutionMarker();
        firstServices.AddSingleton(firstMarker);
        firstServices.AddTransient<RegisteredDependencyTask>();

        var secondServices = new ServiceCollection();
        var secondMarker = new ExecutionMarker();
        secondServices.AddSingleton(secondMarker);
        secondServices.AddTransient<RegisteredDependencyTask>();

        using var firstServiceProvider = firstServices.BuildServiceProvider();
        using var secondServiceProvider = secondServices.BuildServiceProvider();

        var result = await new TaskPipeline()
            .WithServiceProvider(firstServiceProvider)
            .WithServiceProvider(secondServiceProvider)
            .AddTask<RegisteredDependencyTask>()
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.False(firstMarker.WasExecuted);
        Assert.True(secondMarker.WasExecuted);
    }

    [Fact]
    public async Task AddParallel_WithServiceProviderExtension_ResolvesTasksFromPipelineServiceProvider()
    {
        var services = new ServiceCollection();
        var marker = new ExecutionMarker();

        services.AddSingleton(marker);
        services.AddTransient<FirstParallelDependencyTask>();
        services.AddTransient<SecondParallelDependencyTask>();

        using var serviceProvider = services.BuildServiceProvider();

        var result = await new TaskPipeline()
            .WithServiceProvider(serviceProvider)
            .AddParallel<FirstParallelDependencyTask, SecondParallelDependencyTask>()
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.True(marker.FirstWasExecuted);
        Assert.True(marker.SecondWasExecuted);
    }

    [Fact]
    public async Task AddBranch_WithServiceProviderExtension_ChildPipelineInheritsServiceProvider()
    {
        var services = new ServiceCollection();
        var marker = new ExecutionMarker();

        services.AddSingleton(marker);
        services.AddTransient<RegisteredDependencyTask>();

        using var serviceProvider = services.BuildServiceProvider();

        var result = await new TaskPipeline()
            .WithServiceProvider(serviceProvider)
            .AddBranch(
                _ => "run",
                branch => branch.When("run", flow => flow.AddTask<RegisteredDependencyTask>()))
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.True(marker.WasExecuted);
    }

    [Fact]
    public void WithServiceProvider_WithNullPipeline_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();
        using var serviceProvider = services.BuildServiceProvider();

        TaskPipeline pipeline = null!;

        Assert.Throws<ArgumentNullException>(() => pipeline.WithServiceProvider(serviceProvider));
    }

    [Fact]
    public void WithServiceProvider_WithNullServiceProvider_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new TaskPipeline().WithServiceProvider(null!));
    }

    [Fact]
    public void TaskPipeline_HasNoPublicServiceProviderConstructor()
    {
        var serviceProviderConstructors = typeof(TaskPipeline)
            .GetConstructors(BindingFlags.Instance | BindingFlags.Public)
            .Where(constructor => constructor
                .GetParameters()
                .Any(parameter => parameter.ParameterType == typeof(IServiceProvider)));

        Assert.Empty(serviceProviderConstructors);
    }

    [Fact]
    public void TaskPipelineServiceProviderExtensions_HasOnlyWithServiceProviderPublicMethodForServiceProviderRegistration()
    {
        var serviceProviderRegistrationMethods = typeof(TaskPipelineServiceProviderExtensions)
            .GetMethods(BindingFlags.Static | BindingFlags.Public)
            .Where(method => method
                .GetParameters()
                .Any(parameter => parameter.ParameterType == typeof(IServiceProvider)))
            .ToList();

        var method = Assert.Single(serviceProviderRegistrationMethods);
        Assert.Equal(nameof(TaskPipelineServiceProviderExtensions.WithServiceProvider), method.Name);
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
}
