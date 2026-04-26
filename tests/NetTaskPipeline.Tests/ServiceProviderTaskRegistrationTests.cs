using NetTaskPipeline;
using Xunit;

namespace NetTaskPipeline.Tests;

public sealed class ServiceProviderTaskRegistrationTests
{
    [Fact]
    public async Task ExecuteAsync_WithRegisteredTaskInstance_ExecutesResolvedTask()
    {
        var serviceProvider = new TestServiceProvider()
            .Add(typeof(RegisteredInjectedTask), new RegisteredInjectedTask());

        var result = await new TaskPipeline()
            .AddTask<RegisteredInjectedTask>(serviceProvider)
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.True(result.Context.Get<bool>("RegisteredInjectedTaskExecuted"));
    }

    [Fact]
    public async Task ExecuteAsync_WithConstructorDependency_ExecutesResolvedTask()
    {
        var serviceProvider = new TestServiceProvider()
            .Add(typeof(ITestMessageProvider), new TestMessageProvider("resolved from service provider"));

        var result = await new TaskPipeline()
            .AddTask<ConstructorInjectedTask>(serviceProvider)
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.Equal("resolved from service provider", result.Context.Get<string>("InjectedMessage"));
    }

    private sealed class TestServiceProvider : IServiceProvider
    {
        private readonly Dictionary<Type, object> _services = new Dictionary<Type, object>();

        public TestServiceProvider Add(Type type, object service)
        {
            _services[type] = service;
            return this;
        }

        public object? GetService(Type serviceType)
        {
            _services.TryGetValue(serviceType, out var service);
            return service;
        }
    }

    private interface ITestMessageProvider
    {
        string Message { get; }
    }

    private sealed class TestMessageProvider : ITestMessageProvider
    {
        public TestMessageProvider(string message)
        {
            Message = message;
        }

        public string Message { get; }
    }

    private sealed class RegisteredInjectedTask : ITask
    {
        public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
        {
            context.Set("RegisteredInjectedTaskExecuted", true);
            return Task.CompletedTask;
        }
    }

    private sealed class ConstructorInjectedTask : ITask
    {
        private readonly ITestMessageProvider _messageProvider;

        public ConstructorInjectedTask(ITestMessageProvider messageProvider)
        {
            _messageProvider = messageProvider;
        }

        public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
        {
            context.Set("InjectedMessage", _messageProvider.Message);
            return Task.CompletedTask;
        }
    }
}
