# NetTaskPipeline: Async Task Pipeline for .NET

[![build](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/build.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/build.yml)
[![tests](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hebermattos/Net-task-pipeline/main/coverage-badge.json)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml)
[![publish-nuget](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/publish-nuget.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/publish-nuget.yml)

A lightweight async task pipeline for .NET with sequential and parallel execution support.

## Features

- Sequential task execution
- Parallel task groups
- Generic task registration with `AddTask<TTask>()`
- Fluent context-based branching
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

Generic task registration requires a public parameterless constructor because the pipeline creates the task instance internally.

```csharp
public sealed class SaveOrderTask : ITask
{
    public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }
}
```

Tasks that require constructor dependencies are not supported by the generic registration API yet. Keep task dependencies in the shared `TaskContext`, or add a factory/DI integration layer on top of the pipeline.

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
    .AddTask<LoadCustomerTask>()
    .AddTask<SendCustomerEmailTask>()
    .ExecuteAsync();
```

You can also create the context before executing the pipeline and pass initial values to it.

```csharp
var context = new TaskContext();
context.Set("CorrelationId", Guid.NewGuid().ToString("N"));
context.Set("RequestedBy", "system");

var result = await new TaskPipeline()
    .AddTask<LoadCustomerTask>()
    .AddTask<SendCustomerEmailTask>()
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

## Fluent context-based branching

Use `AddBranch` when the pipeline needs to choose between multiple flows based on a string value from the shared `TaskContext`.

```csharp
var context = new TaskContext();
context.Set("CustomerType", "premium");

var result = await new TaskPipeline()
    .AddBranch(
        ctx => ctx.Get<string>("CustomerType"),
        branch => branch
            .When<ApplyPremiumDiscountTask, SendPremiumEmailTask>("premium")
            .When<ApplyStandardDiscountTask, SendStandardEmailTask>("standard")
            .When<BlockOrderTask>("blocked")
            .Default<ReviewCustomerManuallyTask>(),
        name: "Customer type decision")
    .AddTask<SaveOrderTask>()
    .ExecuteAsync(context);
```

For a branch with a full sub-pipeline, use the `When` overload that receives a pipeline.

```csharp
await new TaskPipeline()
    .AddBranch(
        ctx => ctx.Get<string>("CustomerType"),
        branch => branch
            .When("premium", premium => premium
                .WithRetry(2)
                .AddTask<ApplyPremiumDiscountTask>()
                .AddParallel<SendPremiumEmailTask, NotifySalesTeamTask>())
            .When("standard", standard => standard
                .AddTask<ApplyStandardDiscountTask>())
            .Default(fallback => fallback
                .AddTask<ReviewCustomerManuallyTask>()))
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
            .When<RequireManagerApprovalTask>("high-value")
            .When<AutoApproveTask>("low-value"),
        name: "Approval decision")
    .ExecuteAsync(context);
```

The lower-level `AddNamedBranch` API is still available when you prefer to pass a dictionary explicitly.

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
- Fluent context-based branching
- Generic task registration
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
- Generic task registration
- Shared context values
- External context injection
- Fluent context-based branching
- Named context-based branching
- Retry behavior
- Timeout behavior
- `StopOnFirstError`
- `ContinueOnError`
- Maximum degree of parallelism

## Test coverage

The `tests` workflow generates a Cobertura report and updates `coverage-badge.json` on pushes to `main`.

The coverage badge uses this Shields endpoint:

```text
https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hebermattos/Net-task-pipeline/main/coverage-badge.json
```

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
