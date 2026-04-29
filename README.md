# NetTaskPipeline: Async Task Pipeline for .NET

[![build](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/build.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/build.yml)
[![tests](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hebermattos/Net-task-pipeline/main/coverage-badge.json)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml)

A lightweight async task pipeline for .NET with sequential and parallel execution support.

👉 Supports Dependency Injection through fluent service provider registration

## Features

- Sequential task execution
- Parallel task groups
- Generic task registration with `AddTask<TTask>()`
- Inline task registration with delegate-based `AddTask(...)` overloads
- RabbitMQ RPC task execution with delegate-based `AddTaskRpc<TRequest, TResponse>(...)`
- HTTP task execution with delegate-based `AddTaskHttp<TRequest, TResponse>(...)`
- Fluent context-based branching
- Shared execution context
- Cancellation support
- Retry support
- Timeout support
- Error handling modes
- Execution result reporting
- Maximum degree of parallelism for parallel groups
- Runnable simple, advanced, branching, and RabbitMQ RPC Docker examples

## Basic usage

```csharp
using NetTaskPipeline;

var result = await new TaskPipeline()
    .OnError(ErrorMode.StopOnFirstError)
    .WithRetry(2)
    .WithTimeout(TimeSpan.FromSeconds(10))
    .WithMaxDegreeOfParallelism(3)
    .AddTask<ValidateCustomerTask>()
    .AddParallel<GeneratePdfTask, SendEmailTask, SaveLogTask>()
    .ExecuteAsync();

Console.WriteLine($"Pipeline success: {result.Success}");
```

## Creating a task

```csharp
using NetTaskPipeline;

public sealed class ValidateCustomerTask : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        await Task.Delay(500, cancellationToken);

        context.Set("CustomerId", 123);
    }
}
```

## Generic task registration

Use `AddTask<TTask>()` to add a sequential task without creating the instance manually.

```csharp
await new TaskPipeline()
    .AddTask<ValidateCustomerTask>()
    .AddTask<LoadCustomerTask>()
    .ExecuteAsync();
```

Use `AddParallel<TTask1, TTask2>()` or `AddParallel<TTask1, TTask2, TTask3>()` to add a parallel task group by type.

```csharp
await new TaskPipeline()
    .AddTask<ValidateCustomerTask>()
    .AddParallel<GeneratePdfTask, SendEmailTask, SaveLogTask>()
    .AddTask<SaveOrderTask>()
    .ExecuteAsync();
```

Generic task registration requires a public parameterless constructor when NOT using dependency injection.

## Inline task registration

Use the delegate-based `AddTask(...)` overloads when you need a small task without creating a dedicated `ITask` class.

```csharp
var result = await new TaskPipeline()
    .AddTask("Set customer", context =>
    {
        context.Set("CustomerId", 123);
        return Task.CompletedTask;
    })
    .AddTask("Send notification", async (context, cancellationToken) =>
    {
        var customerId = context.Get<int>("CustomerId");

        await Task.Delay(500, cancellationToken);

        Console.WriteLine($"Notification sent for customer {customerId}.");
    })
    .ExecuteAsync();
```

Inline tasks support the same retry and timeout settings as regular tasks.

```csharp
await new TaskPipeline()
    .AddTask(
        "Call external API",
        async (_, cancellationToken) =>
        {
            await Task.Delay(500, cancellationToken);
        },
        retryCount: 3,
        timeout: TimeSpan.FromSeconds(5))
    .ExecuteAsync();
```

When no task name is provided, the default name is `InlineTask`.

```csharp
await new TaskPipeline()
    .AddTask(context =>
    {
        context.Set("StartedAt", DateTimeOffset.UtcNow);
        return Task.CompletedTask;
    })
    .ExecuteAsync();
```

### Dependency Injection

If your tasks require constructor dependencies, register the service provider with `WithServiceProvider(...)` once and use the generic task methods normally. Do not pass the service provider to `AddTask`.

```csharp
using Microsoft.Extensions.DependencyInjection;
using NetTaskPipeline;

var services = new ServiceCollection();

services.AddSingleton<ICustomerRepository, CustomerRepository>();
services.AddTransient<LoadCustomerTask>();
services.AddTransient<SendCustomerNotificationTask>();

using var serviceProvider = services.BuildServiceProvider();

await new TaskPipeline()
    .WithServiceProvider(serviceProvider)
    .AddTask<LoadCustomerTask>()
    .AddTask<SendCustomerNotificationTask>()
    .ExecuteAsync();
```

Tasks are resolved internally using:

```csharp
ActivatorUtilities.GetServiceOrCreateInstance(serviceProvider, taskType)
```

## Shared context

Every pipeline execution uses a `TaskContext`. The same context instance is passed to each task, so values written by one task can be read by later tasks, branch selectors, RabbitMQ RPC tasks, and HTTP tasks.

When `ExecuteAsync()` is called without arguments, the pipeline creates a new empty context. When the caller needs to provide initial data, create a `TaskContext` and pass it to `ExecuteAsync(context)`.

```csharp
var context = new TaskContext();
context.Set("CorrelationId", Guid.NewGuid().ToString("N"));
context.Set("RequestedBy", "system");

var result = await new TaskPipeline()
    .AddTask<LoadCustomerTask>()
    .AddTask<SendCustomerEmailTask>()
    .ExecuteAsync(context);
```

The final context is available through the pipeline result:

```csharp
var correlationId = result.Context.Get<string>("CorrelationId");
```

## Adding values to the shared context

Use `context.Set(key, value)` inside any task to add or replace a value in the shared pipeline context.

```csharp
using NetTaskPipeline;

public sealed class LoadCustomerTask : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        await Task.Delay(500, cancellationToken);

        context.Set("CustomerId", 123);
        context.Set("CustomerName", "John Smith");
        context.Set("CustomerRequest", new GetCustomerRequest { CustomerId = 123 });
        context.Set("CustomerType", "premium");
    }
}
```

A later task can read the values written by `LoadCustomerTask`:

```csharp
await new TaskPipeline()
    .AddTask<LoadCustomerTask>()
    .AddTask<SendCustomerEmailTask>()
    .ExecuteAsync();
```

## Reading values from the shared context

Use `context.Get<T>(key)` when a value is required. It returns the value using the expected type and throws if the key is missing or if the stored value is not compatible with `T`.

```csharp
using NetTaskPipeline;

public sealed class SendCustomerEmailTask : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        var customerId = context.Get<int>("CustomerId");
        var customerName = context.Get<string>("CustomerName");

        await Task.Delay(1000, cancellationToken);

        Console.WriteLine($"Email sent for customer {customerId} - {customerName}.");
    }
}
```

Use `context.TryGet<T>(key, out var value)` when the value is optional.

```csharp
public sealed class AuditTask : ITask
{
    public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        if (context.TryGet<string>("CorrelationId", out var correlationId))
        {
            Console.WriteLine($"Correlation ID: {correlationId}");
        }

        return Task.CompletedTask;
    }
}
```

## Context usage with branching

`AddBranch` can choose the next flow from a value stored in `TaskContext`. Every `When` option receives an `Action<TaskPipeline>` so each branch can configure one or more tasks.

```csharp
var context = new TaskContext();
context.Set("CustomerType", "premium");

var result = await new TaskPipeline()
    .AddBranch(
        selector: ctx => ctx.Get<string>("CustomerType"),
        configure: branch => branch
            .When("premium", flow => flow
                .AddTask<ApplyPremiumDiscountTask>()
                .AddTask<SendPremiumEmailTask>())
            .When("standard", flow => flow
                .AddTask<ApplyStandardDiscountTask>()
                .AddTask<SendStandardEmailTask>())
            .When("blocked", flow => flow
                .AddTask<BlockOrderTask>())
            .Default(flow => flow
                .AddTask<ReviewCustomerManuallyTask>()),
        name: "Customer type decision")
    .AddTask<SaveOrderTask>()
    .ExecuteAsync(context);
```

The branch selector can also be asynchronous.

```csharp
await new TaskPipeline()
    .AddBranch(
        async (ctx, cancellationToken) =>
        {
            await Task.Delay(100, cancellationToken);

            return ctx.Get<decimal>("Total") >= 1000m
                ? "high-value"
                : "low-value";
        },
        branch => branch
            .When("high-value", flow => flow.AddTask<RequireManagerApprovalTask>())
            .When("low-value", flow => flow.AddTask<AutoApproveTask>()),
        name: "Approval decision")
    .ExecuteAsync(context);
```

## Context usage with RabbitMQ RPC tasks

Use `AddTaskRpc<TRequest, TResponse>(requestFactory, configure)` when the pipeline needs to send a typed RabbitMQ RPC request.

```csharp
using NetTaskPipeline;

var context = new TaskContext();

var result = await new TaskPipeline()
    .AddTaskRpc<GetCustomerRequest, GetCustomerResponse>(
        ctx => new GetCustomerRequest
        {
            CustomerId = ctx.Get<int>("CustomerId")
        },
        options =>
        {
            options.ConnectionUri = "amqp://guest:guest@localhost:5672/";
            options.RoutingKey = "CustomerRequest";
            options.ResponseKey = "CustomerResponse";
        })
    .ExecuteAsync(context);
```

The RabbitMQ RPC response is deserialized as `TResponse` and stored automatically using the configured `ResponseKey`.

```csharp
var response = result.Context.Get<GetCustomerResponse>("CustomerResponse");
```

## Context usage with HTTP tasks

Use `AddTaskHttp<TRequest, TResponse>(requestFactory, configure)` when the pipeline needs to send a typed HTTP request and store the typed response in the shared context.

```csharp
using System.Net.Http;
using NetTaskPipeline;

var context = new TaskContext();
context.Set("CustomerId", 123);

var result = await new TaskPipeline()
    .AddTaskHttp<GetCustomerRequest, GetCustomerResponse>(
        ctx => new GetCustomerRequest
        {
            CustomerId = ctx.Get<int>("CustomerId")
        },
        options =>
        {
            options.RequestUri = "https://api.example.com/customers";
            options.Method = HttpMethod.Post;
            options.ResponseKey = "CustomerResponse";
            options.Headers["x-api-key"] = "secret";
        })
    .ExecuteAsync(context);
```

The HTTP response is deserialized as `TResponse` and stored automatically using the configured `ResponseKey`.

```csharp
var response = result.Context.Get<GetCustomerResponse>("CustomerResponse");
```

When the endpoint returns plain text, use `string` as the response type.

```csharp
var result = await new TaskPipeline()
    .AddTaskHttp<object, string>(
        _ => new object(),
        options =>
        {
            options.RequestUri = "https://api.example.com/health";
            options.Method = HttpMethod.Get;
            options.ResponseKey = "HealthStatus";
        })
    .ExecuteAsync();

var healthStatus = result.Context.Get<string>("HealthStatus");
```

```csharp
public sealed class GetCustomerRequest
{
    public int CustomerId { get; set; }
}

public sealed class GetCustomerResponse
{
    public int CustomerId { get; set; }

    public string Name { get; set; } = string.Empty;
}
```

## Execution model

Each `AddTask` call creates one execution group.

```csharp
await new TaskPipeline()
    .AddTask<FirstTask>()
    .AddParallel<SecondTask, ThirdTask>()
    .AddTask<FourthTask>()
    .ExecuteAsync();
```

Execution order:

```text
FirstTask
  ↓
SecondTask + ThirdTask in parallel
  ↓
FourthTask
```

## Error handling

```csharp
await new TaskPipeline()
    .OnError(ErrorMode.ContinueOnError)
    .AddTask<FirstTask>()
    .AddTask<SecondTask>()
    .ExecuteAsync();
```

Available modes:

- `StopOnFirstError`
- `ContinueOnError`

## Retry

```csharp
await new TaskPipeline()
    .WithRetry(3)
    .AddTask<CallExternalApiTask>()
    .ExecuteAsync();
```

Per-task retry:

```csharp
await new TaskPipeline()
    .AddTask<CallExternalApiTask>(retryCount: 3)
    .ExecuteAsync();
```

## Timeout

```csharp
await new TaskPipeline()
    .WithTimeout(TimeSpan.FromSeconds(30))
    .AddTask<LongRunningTask>()
    .ExecuteAsync();
```

Per-task timeout:

```csharp
await new TaskPipeline()
    .AddTask<LongRunningTask>(timeout: TimeSpan.FromSeconds(5))
    .ExecuteAsync();
```

## Results

```csharp
TaskPipelineResult result = await pipeline.ExecuteAsync();

foreach (var taskResult in result.TaskResults)
{
    Console.WriteLine($"{taskResult.TaskName}: {taskResult.Status} in {taskResult.Duration}");
}
```

## Runnable examples

The repository includes runnable examples.

```bash
dotnet run --project examples/SimpleExample/SimpleExample.csproj
dotnet run --project examples/AdvancedExample/AdvancedExample.csproj
dotnet run --project examples/BranchingExample/BranchingExample.csproj
```

The RabbitMQ RPC Docker example can be started with Docker Compose:

```bash
cd examples/RpcDockerExample
docker compose up --build
```

## License

MIT
