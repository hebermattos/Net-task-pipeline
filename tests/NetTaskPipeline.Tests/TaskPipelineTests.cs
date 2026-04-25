using System.Diagnostics;
using NetTaskPipeline;
using Xunit;

namespace NetTaskPipeline.Tests;

public sealed class TaskPipelineTests
{
    [Fact]
    public async Task ExecuteAsync_WithSequentialTasks_ExecutesInOrder()
    {
        var executionOrder = new List<string>();

        var result = await new TaskPipeline()
            .AddTask(new DelegateTask("First", _ =>
            {
                executionOrder.Add("First");
                return Task.CompletedTask;
            }))
            .AddTask(new DelegateTask("Second", _ =>
            {
                executionOrder.Add("Second");
                return Task.CompletedTask;
            }))
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.Equal(new[] { "First", "Second" }, executionOrder);
        Assert.Equal(2, result.TaskResults.Count);
    }

    [Fact]
    public async Task ExecuteAsync_WithParallelGroup_ExecutesTasksInParallel()
    {
        var stopwatch = Stopwatch.StartNew();

        var result = await new TaskPipeline()
            .AddTask(
                new DelayTask("Delay A", TimeSpan.FromMilliseconds(700)),
                new DelayTask("Delay B", TimeSpan.FromMilliseconds(700)))
            .ExecuteAsync();

        stopwatch.Stop();

        Assert.True(result.Success);
        Assert.Equal(2, result.TaskResults.Count);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromMilliseconds(1200));
    }

    [Fact]
    public async Task ExecuteAsync_WithSharedContext_AllowsTasksToShareValues()
    {
        var result = await new TaskPipeline()
            .AddTask(new DelegateTask("Set customer", context =>
            {
                context.Set("CustomerId", 123);
                return Task.CompletedTask;
            }))
            .AddTask(new DelegateTask("Read customer", context =>
            {
                var customerId = context.Get<int>("CustomerId");
                context.Set("Message", $"Customer {customerId}");
                return Task.CompletedTask;
            }))
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.Equal("Customer 123", result.Context.Get<string>("Message"));
    }

    [Fact]
    public async Task ExecuteAsync_WithExternalContext_UsesProvidedContext()
    {
        var context = new TaskContext();
        context.Set("CorrelationId", "abc-123");

        var result = await new TaskPipeline()
            .AddTask(new DelegateTask("Read correlation", ctx =>
            {
                var correlationId = ctx.Get<string>("CorrelationId");
                ctx.Set("ReadValue", correlationId);
                return Task.CompletedTask;
            }))
            .ExecuteAsync(context);

        Assert.True(result.Success);
        Assert.Same(context, result.Context);
        Assert.Equal("abc-123", result.Context.Get<string>("ReadValue"));
    }

    [Fact]
    public async Task ExecuteAsync_WithRetry_RetriesFailedTask()
    {
        var attempts = 0;

        var result = await new TaskPipeline()
            .AddTask(new DelegateTask("Unstable", _ =>
            {
                attempts++;

                if (attempts < 3)
                    throw new InvalidOperationException("Temporary failure.");

                return Task.CompletedTask;
            }), retryCount: 2)
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.Equal(3, attempts);
        Assert.Equal(3, result.TaskResults.Single().Attempts);
    }

    [Fact]
    public async Task ExecuteAsync_WithTimeout_ReturnsFailedResult()
    {
        var result = await new TaskPipeline()
            .AddTask(new DelayTask("Slow task", TimeSpan.FromSeconds(3)), timeout: TimeSpan.FromMilliseconds(100))
            .ExecuteAsync();

        var taskResult = Assert.Single(result.TaskResults);

        Assert.False(result.Success);
        Assert.Equal(TaskExecutionStatus.Failed, taskResult.Status);
        Assert.IsType<TimeoutException>(taskResult.Exception);
    }

    [Fact]
    public async Task ExecuteAsync_WithStopOnFirstError_StopsNextGroups()
    {
        var executedAfterFailure = false;

        var result = await new TaskPipeline()
            .OnError(ErrorMode.StopOnFirstError)
            .AddTask(new DelegateTask("Fail", _ => throw new InvalidOperationException("Failure.")))
            .AddTask(new DelegateTask("Should not run", _ =>
            {
                executedAfterFailure = true;
                return Task.CompletedTask;
            }))
            .ExecuteAsync();

        Assert.False(result.Success);
        Assert.False(executedAfterFailure);
        Assert.Single(result.TaskResults);
    }

    [Fact]
    public async Task ExecuteAsync_WithContinueOnError_ExecutesNextGroups()
    {
        var executedAfterFailure = false;

        var result = await new TaskPipeline()
            .OnError(ErrorMode.ContinueOnError)
            .AddTask(new DelegateTask("Fail", _ => throw new InvalidOperationException("Failure.")))
            .AddTask(new DelegateTask("Should run", _ =>
            {
                executedAfterFailure = true;
                return Task.CompletedTask;
            }))
            .ExecuteAsync();

        Assert.False(result.Success);
        Assert.True(executedAfterFailure);
        Assert.Equal(2, result.TaskResults.Count);
    }

    [Fact]
    public async Task ExecuteAsync_WithMaxDegreeOfParallelism_LimitsConcurrentTasks()
    {
        var running = 0;
        var maxRunning = 0;

        async Task ExecuteLimitedTask(TaskContext _, CancellationToken cancellationToken)
        {
            var current = Interlocked.Increment(ref running);
            UpdateMax(ref maxRunning, current);

            try
            {
                await Task.Delay(200, cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref running);
            }
        }

        var result = await new TaskPipeline()
            .WithMaxDegreeOfParallelism(2)
            .AddTask(
                new DelegateTask("Task 1", ExecuteLimitedTask),
                new DelegateTask("Task 2", ExecuteLimitedTask),
                new DelegateTask("Task 3", ExecuteLimitedTask),
                new DelegateTask("Task 4", ExecuteLimitedTask))
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.Equal(2, maxRunning);
    }

    [Fact]
    public async Task ExecuteAsync_WithBranchTrue_ExecutesTrueFlow()
    {
        var context = new TaskContext();
        context.Set("IsPremiumCustomer", true);

        var result = await new TaskPipeline()
            .AddBranch(
                ctx => ctx.Get<bool>("IsPremiumCustomer"),
                whenTrue: branch => branch.AddTask(new DelegateTask("Premium flow", ctx =>
                {
                    ctx.Set("SelectedFlow", "premium");
                    return Task.CompletedTask;
                })),
                whenFalse: branch => branch.AddTask(new DelegateTask("Standard flow", ctx =>
                {
                    ctx.Set("SelectedFlow", "standard");
                    return Task.CompletedTask;
                })),
                name: "Customer type decision")
            .ExecuteAsync(context);

        Assert.True(result.Success);
        Assert.Equal("premium", result.Context.Get<string>("SelectedFlow"));
        Assert.Contains(result.TaskResults, taskResult => taskResult.TaskName == "Premium flow");
        Assert.DoesNotContain(result.TaskResults, taskResult => taskResult.TaskName == "Standard flow");
    }

    [Fact]
    public async Task ExecuteAsync_WithBranchFalse_ExecutesFalseFlow()
    {
        var context = new TaskContext();
        context.Set("IsPremiumCustomer", false);

        var result = await new TaskPipeline()
            .AddBranch(
                ctx => ctx.Get<bool>("IsPremiumCustomer"),
                whenTrue: branch => branch.AddTask(new DelegateTask("Premium flow", ctx =>
                {
                    ctx.Set("SelectedFlow", "premium");
                    return Task.CompletedTask;
                })),
                whenFalse: branch => branch.AddTask(new DelegateTask("Standard flow", ctx =>
                {
                    ctx.Set("SelectedFlow", "standard");
                    return Task.CompletedTask;
                })),
                name: "Customer type decision")
            .ExecuteAsync(context);

        Assert.True(result.Success);
        Assert.Equal("standard", result.Context.Get<string>("SelectedFlow"));
        Assert.Contains(result.TaskResults, taskResult => taskResult.TaskName == "Standard flow");
        Assert.DoesNotContain(result.TaskResults, taskResult => taskResult.TaskName == "Premium flow");
    }

    [Fact]
    public async Task ExecuteAsync_WithAsyncBranchCondition_ExecutesSelectedFlow()
    {
        var context = new TaskContext();
        context.Set("Total", 1500m);

        var result = await new TaskPipeline()
            .AddBranch(
                async (ctx, cancellationToken) =>
                {
                    await Task.Delay(50, cancellationToken);
                    return ctx.Get<decimal>("Total") >= 1000m;
                },
                whenTrue: branch => branch.AddTask(new DelegateTask("High value flow", ctx =>
                {
                    ctx.Set("RequiresApproval", true);
                    return Task.CompletedTask;
                })),
                whenFalse: branch => branch.AddTask(new DelegateTask("Low value flow", ctx =>
                {
                    ctx.Set("RequiresApproval", false);
                    return Task.CompletedTask;
                })),
                name: "Approval decision")
            .ExecuteAsync(context);

        Assert.True(result.Success);
        Assert.True(result.Context.Get<bool>("RequiresApproval"));
        Assert.Contains(result.TaskResults, taskResult => taskResult.TaskName == "High value flow");
    }

    [Fact]
    public async Task ExecuteAsync_WithBranchConditionFailure_ReturnsFailedResult()
    {
        var result = await new TaskPipeline()
            .AddBranch(
                _ => throw new InvalidOperationException("The branch condition failed."),
                whenTrue: branch => branch.AddTask(new DelegateTask("Should not run", _ => Task.CompletedTask)),
                name: "Failing decision")
            .ExecuteAsync();

        var taskResult = Assert.Single(result.TaskResults);

        Assert.False(result.Success);
        Assert.Equal("Failing decision", taskResult.TaskName);
        Assert.Equal(TaskExecutionStatus.Failed, taskResult.Status);
        Assert.IsType<InvalidOperationException>(taskResult.Exception);
    }

    private static void UpdateMax(ref int target, int value)
    {
        int initialValue;
        int computedValue;

        do
        {
            initialValue = target;
            computedValue = Math.Max(initialValue, value);
        }
        while (initialValue != Interlocked.CompareExchange(ref target, computedValue, initialValue));
    }

    private sealed class DelegateTask : ITask
    {
        private readonly Func<TaskContext, CancellationToken, Task> _execute;

        public DelegateTask(string name, Func<TaskContext, Task> execute)
            : this(name, (context, _) => execute(context))
        {
        }

        public DelegateTask(string name, Func<TaskContext, CancellationToken, Task> execute)
        {
            Name = name;
            _execute = execute;
        }

        public string Name { get; }

        public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
        {
            return _execute(context, cancellationToken);
        }
    }

    private sealed class DelayTask : ITask
    {
        private readonly TimeSpan _delay;

        public DelayTask(string name, TimeSpan delay)
        {
            Name = name;
            _delay = delay;
        }

        public string Name { get; }

        public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
        {
            await Task.Delay(_delay, cancellationToken);
        }
    }
}
