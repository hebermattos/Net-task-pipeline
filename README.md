# NetTaskPipeline

[![build](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/build.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/build.yml)
[![tests](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml)

A lightweight async task pipeline for .NET with sequential and parallel execution support.

## Features

- Sequential task execution
- Parallel task groups
- Shared execution context
- Cancellation support
- Retry support
- Timeout support
- Error handling modes
- Execution result reporting
- Maximum degree of parallelism for parallel groups
- NuGet-ready project configuration
- Runnable simple and advanced examples
- Unit tests with xUnit

## Installation

After publishing the package to NuGet:

```bash
dotnet add package NetTaskPipeline
```

## Basic usage

```csharp
using NetTaskPipeline;

var result = await new TaskPipeline()
    .OnError(ErrorMode.StopOnFirstError)
    .WithRetry(2)
    .WithTimeout(TimeSpan.FromSeconds(10))
    .WithMaxDegreeOfParallelism(3)
    .AddTask(new ValidateCustomerTask())
    .AddTask(
        new GeneratePdfTask(),
        new SendEmailTask(),
        new SaveLogTask())
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
        context.Set("IsActive", true);
    }
}
```

Every task receives the same `TaskContext` instance during a pipeline execution, so values added by one task can be read by later tasks.

```csharp
await new TaskPipeline()
    .AddTask(new LoadCustomerTask())
    .AddTask(new SendCustomerEmailTask())
    .ExecuteAsync();
```

You can also create the context before executing the pipeline and pass initial values to it.

```csharp
var context = new TaskContext();
context.Set("CorrelationId", Guid.NewGuid().ToString("N"));
context.Set("RequestedBy", "system");

var result = await new TaskPipeline()
    .AddTask(new LoadCustomerTask())
    .AddTask(new SendCustomerEmailTask())
    .ExecuteAsync(context);
```

## Reading from the shared context

Use `context.Get<T>(key)` when the value is required. It throws an exception if the key does not exist or if the value has a different type.

```csharp
using NetTaskPipeline;

public sealed class SendEmailTask : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        var customerId = context.Get<int>("CustomerId");

        await Task.Delay(1000, cancellationToken);

        Console.WriteLine($"Email sent for customer {customerId}.");
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

## Execution model

Each `AddTask` call creates one execution group.

```csharp
await new TaskPipeline()
    .AddTask(new FirstTask())
    .AddTask(new SecondTask(), new ThirdTask())
    .AddTask(new FourthTask())
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
    .AddTask(new FirstTask())
    .AddTask(new SecondTask())
    .ExecuteAsync();
```

Available modes:

- `StopOnFirstError`
- `ContinueOnError`

## Retry

```csharp
await new TaskPipeline()
    .WithRetry(3)
    .AddTask(new CallExternalApiTask())
    .ExecuteAsync();
```

Per-task retry:

```csharp
await new TaskPipeline()
    .AddTask(new CallExternalApiTask(), retryCount: 3)
    .ExecuteAsync();
```

## Timeout

```csharp
await new TaskPipeline()
    .WithTimeout(TimeSpan.FromSeconds(30))
    .AddTask(new LongRunningTask())
    .ExecuteAsync();
```

Per-task timeout:

```csharp
await new TaskPipeline()
    .AddTask(new LongRunningTask(), timeout: TimeSpan.FromSeconds(5))
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

The repository includes two executable examples.

### Simple example

Covers sequential and parallel execution with a final result summary.

```bash
dotnet run --project examples/SimpleExample/SimpleExample.csproj
```

### Advanced example

Covers most pipeline features in a single runnable flow:

- Shared `TaskContext`
- Sequential execution
- Parallel task groups
- `ContinueOnError`
- Global retry
- Per-task retry
- Global timeout
- Per-task timeout
- Maximum degree of parallelism
- Execution result report

```bash
dotnet run --project examples/AdvancedExample/AdvancedExample.csproj
```

## Build

```bash
dotnet build
```

## Tests

```bash
dotnet test
```

Or run the test project directly:

```bash
dotnet test tests/NetTaskPipeline.Tests/NetTaskPipeline.Tests.csproj
```

The test suite covers:

- Sequential execution order
- Parallel task groups
- Shared context values
- External context injection
- Retry behavior
- Timeout behavior
- `StopOnFirstError`
- `ContinueOnError`
- Maximum degree of parallelism

## Pack

```bash
dotnet pack src/NetTaskPipeline/NetTaskPipeline.csproj -c Release
```

## License

MIT
