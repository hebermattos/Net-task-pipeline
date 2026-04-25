# NetTaskPipeline

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

## Reading from the shared context

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

## Build

```bash
dotnet build
```

## Pack

```bash
dotnet pack src/NetTaskPipeline/NetTaskPipeline.csproj -c Release
```

## License

MIT
