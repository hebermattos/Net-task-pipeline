# NetTaskPipeline

[![NuGet](https://img.shields.io/nuget/v/NetTaskPipeline.svg)](https://www.nuget.org/packages/NetTaskPipeline)
[![examples](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml)
[![build and tests](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml)
![coverage](https://img.shields.io/badge/coverage-%E2%89%A580%25-green)

A lightweight async task pipeline for .NET with sequential and parallel execution, branching, retries, timeouts, Dependency Injection, HTTP tasks, and RabbitMQ RPC.

## Installation

```bash
dotnet add package NetTaskPipeline
```

## Quick start

```csharp
using NetTaskPipeline;

var result = await new TaskPipeline()
    .AddTask("Load customer", context =>
    {
        context.Set("CustomerId", 123);
        return Task.CompletedTask;
    })
    .AddTask("Process customer", context =>
    {
        Console.WriteLine(context.Get<int>("CustomerId"));
        return Task.CompletedTask;
    })
    .ExecuteAsync();

Console.WriteLine($"Pipeline success: {result.Success}");
```

## Capabilities

| Capability | API |
| --- | --- |
| Sequential tasks | `AddTask(...)` / `AddTask<TTask>()` |
| Parallel tasks | `AddParallel<...>()` |
| Branching | `AddBranch(...)` |
| Shared state | `TaskContext` |
| Retry | `WithRetry(...)` / per-task `retryCount` |
| Retry delay/backoff | `WithRetryDelay(...)` |
| Retry filtering | `WithRetryPolicy(...)` |
| Timeout | `WithTimeout(...)` / per-task `timeout` |
| Error handling | `OnError(...)` |
| Dependency Injection | `WithServiceProvider(...)` |
| HTTP | `AddTaskHttp<...>()` |
| RabbitMQ RPC | `AddTaskRpc<...>()` |

## Core execution

Each `AddTask` creates a sequential execution group. `AddParallel` runs tasks in the same group concurrently before the next group starts.

```csharp
await new TaskPipeline()
    .AddTask<ValidateCustomerTask>()
    .AddParallel<GeneratePdfTask, SendEmailTask, SaveLogTask>()
    .AddTask<SaveOrderTask>()
    .ExecuteAsync();
```

```text
ValidateCustomerTask
        ↓
GeneratePdfTask + SendEmailTask + SaveLogTask
        ↓
SaveOrderTask
```

A task implements `ITask`:

```csharp
public sealed class ValidateCustomerTask : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        await Task.Delay(500, cancellationToken);
        context.Set("CustomerId", 123);
    }
}
```

Generic registration requires a public parameterless constructor unless Dependency Injection is configured. Small operations can instead use the delegate-based `AddTask(...)` overloads shown in the quick start.

## TaskContext

One `TaskContext` is shared by the complete pipeline. Use `Set` to add or replace data, `Get<T>` for required values, and `TryGet<T>` for optional values.

```csharp
var context = new TaskContext();
context.Set("CorrelationId", Guid.NewGuid().ToString("N"));
context.Set("CustomerId", 123);

var result = await new TaskPipeline()
    .AddTask<LoadCustomerTask>()
    .AddTask("Audit", ctx =>
    {
        if (ctx.TryGet<string>("CorrelationId", out var correlationId))
            Console.WriteLine(correlationId);

        return Task.CompletedTask;
    })
    .ExecuteAsync(context);

var customerId = result.Context.Get<int>("CustomerId");
```

The final context is available from `TaskPipelineResult.Context`. `GetOrAdd` uses `ConcurrentDictionary` semantics: insertion is atomic, but its value factory may execute more than once under contention, so factories should not contain side effects.

## Branching

Branches select a pipeline flow from the shared context.

```csharp
await new TaskPipeline()
    .AddBranch(
        selector: ctx => ctx.Get<string>("CustomerType"),
        configure: branch => branch
            .When("premium", flow => flow.AddTask<ApplyPremiumDiscountTask>())
            .When("standard", flow => flow.AddTask<ApplyStandardDiscountTask>())
            .Default(flow => flow.AddTask<ReviewCustomerManuallyTask>()),
        name: "Customer type decision")
    .AddTask<SaveOrderTask>()
    .ExecuteAsync(context);
```

The selector also has an asynchronous overload that receives a `CancellationToken`. If no case matches and no `Default` flow is configured, the branch returns a failed execution result instead of silently succeeding.

## Reliability

Configure retry, optional retry delay/backoff, timeout, and error handling globally:

```csharp
await new TaskPipeline()
    .OnError(ErrorMode.StopOnFirstError)
    .WithRetry(3)
    .WithRetryDelay(TimeSpan.FromMilliseconds(250), exponentialBackoff: true)
    .WithTimeout(TimeSpan.FromSeconds(30))
    .AddTask<CallExternalApiTask>()
    .ExecuteAsync();
```

Without `WithRetryDelay`, retries are immediate. Exponential backoff is overflow-safe, and optional jitter can spread concurrent retries (`WithRetryDelay(delay, exponentialBackoff: true, jitter: true)`). Use `WithRetryPolicy(exception => ...)` to retry only selected failures. External cancellation is never retried; task timeouts remain failures and can be filtered by the retry policy. Timeouts are cooperative: the pipeline cancels the token at the configured deadline, so tasks should observe the supplied `CancellationToken`. If a task ignores cancellation, the pipeline waits for it to return and still records a timeout failure once it completes. Per-task settings override the pipeline defaults where supported:

```csharp
await new TaskPipeline()
    .AddTask<CallExternalApiTask>(
        retryCount: 3,
        timeout: TimeSpan.FromSeconds(5))
    .ExecuteAsync();
```

Error modes are `StopOnFirstError` and `ContinueOnError`.

## Integrations

### HTTP

`AddTaskHttp<TRequest, TResponse>` sends a typed HTTP request and stores the typed response in `TaskContext`.

```csharp
var result = await new TaskPipeline()
    .AddTaskHttp<GetCustomerRequest, GetCustomerResponse>(
        ctx => new GetCustomerRequest { CustomerId = ctx.Get<int>("CustomerId") },
        options =>
        {
            options.RequestUri = "https://api.example.com/customers";
            options.Method = HttpMethod.Post;
            options.ResponseKey = "CustomerResponse";
        })
    .ExecuteAsync(context);

var response = result.Context.Get<GetCustomerResponse>("CustomerResponse");
```

Use `string` as `TResponse` for plain-text responses.

### RabbitMQ RPC

`AddTaskRpc<TRequest, TResponse>` publishes a typed request, waits for the correlated response, deserializes it, and stores it in `TaskContext`.

```csharp
var result = await new TaskPipeline()
    .AddTaskRpc<GetCustomerRequest, GetCustomerResponse>(
        ctx => new GetCustomerRequest { CustomerId = ctx.Get<int>("CustomerId") },
        options =>
        {
            options.ConnectionUri = "amqp://guest:guest@localhost:5672/";
            options.RoutingKey = "CustomerRequest";
            options.ResponseKey = "CustomerResponse";
        })
    .ExecuteAsync(context);

var response = result.Context.Get<GetCustomerResponse>("CustomerResponse");
```

## Dependency Injection

Configure the service provider once. Typed tasks are then resolved from it, including tasks inside parallel groups and branches.

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

Internally, task resolution uses `ActivatorUtilities.GetServiceOrCreateInstance`.

## Results

`ExecuteAsync` returns a `TaskPipelineResult` containing the final context and task execution results. Execution result properties are read-only to consumers so recorded status, attempts, timing, and exceptions cannot be mutated after execution.

```csharp
var result = await pipeline.ExecuteAsync();

foreach (var taskResult in result.TaskResults)
    Console.WriteLine($"{taskResult.TaskName}: {taskResult.Status} in {taskResult.Duration}");
```

## Examples

| Example | Purpose |
| --- | --- |
| `SimpleExample` | Basic sequential pipeline |
| `AdvancedExample` | Advanced pipeline behavior |
| `BranchingExample` | Context-based branching |
| `DependencyInjectionExample` | Service-provider task resolution |
| `HttpExample` | Typed HTTP tasks |
| `RpcDockerExample` | RabbitMQ RPC with Docker Compose |

Run the .NET examples from the repository root:

```bash
dotnet run --project examples/SimpleExample/SimpleExample.csproj
dotnet run --project examples/AdvancedExample/AdvancedExample.csproj
dotnet run --project examples/BranchingExample/BranchingExample.csproj
dotnet run --project examples/DependencyInjectionExample/DependencyInjectionExample.csproj
dotnet run --project examples/HttpExample/HttpExample.csproj
```

Run the RabbitMQ RPC example with Docker:

```bash
cd examples/RpcDockerExample
docker compose up --build
```

GitHub Actions executes all examples on pushes and pull requests to `main` and validates their expected results.

## Development

| Check | Behavior |
| --- | --- |
| Build and unit tests | Runs for pushes and pull requests to `main` |
| Unit coverage | Requires at least 80% line coverage for unit-testable code |
| RabbitMQ integration | Runs against a real RabbitMQ service and tests RPC round-trip and timeout behavior |
| Integration coverage | Collected separately as Cobertura and uploaded as a workflow artifact |
| Examples | All runnable examples must produce their expected results |
| Concurrency | Superseded CI runs for the same branch are cancelled |
| NuGet release | Published only from a GitHub Release tag such as `v1.0.0` after the full CI and a packed-package smoke test succeed |
| Package debugging | Source Link and `.snupkg` symbols are published with the package |

RabbitMQ transport/RPC files are excluded from the **unit** coverage gate because they are integration-bound; they are covered separately by the RabbitMQ integration job. The ≥80% badge therefore represents the unit-testable-code gate, not aggregate coverage across every source file.

Code changes should keep tests and examples current. Project documentation is intentionally maintained only in this root `README.md`.

## License

MIT
