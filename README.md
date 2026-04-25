# NetTaskPipeline: Async Task Pipeline for .NET

[![build](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/build.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/build.yml)
[![tests](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml)
[![publish-nuget](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/publish-nuget.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/publish-nuget.yml)

A lightweight async task pipeline for .NET with sequential and parallel execution support.

## Features

- Sequential task execution
- Parallel task groups
- Named context-based branching
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
- GitHub Actions workflow for NuGet publishing

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
        context.Set("CustomerType", "premium");
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

## Named context-based branching

Use `AddNamedBranch` when the pipeline needs to choose between multiple flows based on a string value from the shared `TaskContext`.

```csharp
var context = new TaskContext();
context.Set("CustomerType", "premium");

var result = await new TaskPipeline()
    .AddTask(new LoadCustomerTask())
    .AddNamedBranch(
        branchNameSelector: ctx => ctx.Get<string>("CustomerType"),
        branches: new Dictionary<string, Action<TaskPipeline>>
        {
            ["premium"] = premium => premium
                .AddTask(new ApplyPremiumDiscountTask())
                .AddTask(new SendPremiumEmailTask()),

            ["standard"] = standard => standard
                .AddTask(new ApplyStandardDiscountTask())
                .AddTask(new SendStandardEmailTask()),

            ["blocked"] = blocked => blocked
                .AddTask(new BlockOrderTask())
        },
        defaultBranch: fallback => fallback
            .AddTask(new ReviewCustomerManuallyTask()),
        name: "Customer type decision")
    .AddTask(new SaveOrderTask())
    .ExecuteAsync(context);
```

The branch selector can also be asynchronous.

```csharp
await new TaskPipeline()
    .AddNamedBranch(
        branchNameSelector: async (ctx, cancellationToken) =>
        {
            await Task.Delay(100, cancellationToken);
            return ctx.Get<decimal>("Total") >= 1000m
                ? "high-value"
                : "low-value";
        },
        branches: new Dictionary<string, Action<TaskPipeline>>
        {
            ["high-value"] = highValue => highValue
                .AddTask(new RequireManagerApprovalTask()),

            ["low-value"] = lowValue => lowValue
                .AddTask(new AutoApproveTask())
        },
        name: "Approval decision")
    .ExecuteAsync(context);
```

A named branch can contain a full sub-pipeline, including sequential tasks, parallel groups, retry, timeout, and other branch steps.

```csharp
await new TaskPipeline()
    .AddNamedBranch(
        branchNameSelector: ctx => ctx.Get<string>("DocumentFlow"),
        branches: new Dictionary<string, Action<TaskPipeline>>
        {
            ["required"] = documents => documents
                .AddTask(new ValidateDocumentsTask())
                .AddTask(
                    new GeneratePdfTask(),
                    new NotifyBackOfficeTask()),

            ["skipped"] = noDocuments => noDocuments
                .AddTask(new SkipDocumentValidationTask())
        })
    .ExecuteAsync(context);
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
- Named context-based branching
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
- Named context-based branching
- Retry behavior
- Timeout behavior
- `StopOnFirstError`
- `ContinueOnError`
- Maximum degree of parallelism

## Pack

```bash
dotnet pack src/NetTaskPipeline/NetTaskPipeline.csproj -c Release
```

## Publish to NuGet

The repository includes a GitHub Actions workflow to publish the package to NuGet:

```text
.github/workflows/publish-nuget.yml
```

Before running it, create a repository secret named `NUGET_API_KEY` with your NuGet API key.

GitHub path:

```text
Settings > Secrets and variables > Actions > New repository secret
```

Secret name:

```text
NUGET_API_KEY
```

### Manual publish

Go to:

```text
Actions > publish-nuget > Run workflow
```

Then inform the package version, for example:

```text
0.1.0
```

The workflow will run:

```bash
dotnet restore TaskPipeline.sln
dotnet build TaskPipeline.sln --configuration Release --no-restore
dotnet test tests/NetTaskPipeline.Tests/NetTaskPipeline.Tests.csproj --configuration Release --no-build
dotnet pack src/NetTaskPipeline/NetTaskPipeline.csproj --configuration Release --no-build --output artifacts/packages -p:PackageVersion=$PACKAGE_VERSION
dotnet nuget push "artifacts/packages/*.nupkg" --api-key "$NUGET_API_KEY" --source https://api.nuget.org/v3/index.json --skip-duplicate
```

### Publish by tag

You can also publish by pushing a version tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The package version will be resolved from the tag name, without the leading `v`.

## License

MIT
