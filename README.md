# NetTaskPipeline: Async Task Pipeline for .NET

[![build](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/build.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/build.yml)
[![tests](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hebermattos/Net-task-pipeline/main/coverage-badge.json)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml)

A lightweight async task pipeline for .NET with sequential and parallel execution support.

👉 Supports Dependency Injection (constructor injection built-in)

## Features

- Sequential task execution
- Parallel task groups
- Generic task registration with `AddTask<TTask>()`
- RPC task execution with `AddTaskRpc<TRequest, TResponse>(key)`
- Fluent context-based branching
- Shared execution context
- Cancellation support
- Retry support
- Timeout support
- Error handling modes
- Execution result reporting
- Maximum degree of parallelism for parallel groups
- Runnable simple, advanced, and RabbitMQ RPC Docker examples

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

### Dependency Injection

If your tasks require constructor dependencies, use the DI-enabled overloads:

```csharp
.AddTask<MyTask>(serviceProvider)
.AddParallel<TaskA, TaskB>(serviceProvider)
```

Example:

```csharp
var services = new ServiceCollection();

services.AddSingleton<ICustomerRepository, CustomerRepository>();
services.AddTransient<LoadCustomerTask>();

var serviceProvider = services.BuildServiceProvider();

await new TaskPipeline()
    .AddTask<LoadCustomerTask>(serviceProvider)
    .ExecuteAsync();
```

Tasks are resolved using:

```csharp
ActivatorUtilities.GetServiceOrCreateInstance<TTask>(serviceProvider)
```

## Adding values to the shared context

...