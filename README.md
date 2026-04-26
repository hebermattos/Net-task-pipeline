# NetTaskPipeline: Async Task Pipeline for .NET

[![build](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/build.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/build.yml)
[![tests](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hebermattos/Net-task-pipeline/main/coverage-badge.json)](https://github.com/hebermattos/Net-task-pipeline/actions/workflows/tests.yml)

A lightweight async task pipeline for .NET with sequential and parallel execution support.

## Features

- Sequential task execution
- Parallel task groups
- Generic task registration with `AddTask<TTask>()`
- RPC task execution with `AddTaskRpc(key)`
- Fluent context-based branching
- Shared execution context
- Cancellation support
- Retry support
- Timeout support
- Error handling modes
- Execution result reporting
- Maximum degree of parallelism for parallel groups
- Runnable simple and advanced examples

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
        context.Set("CustomerRequest", new GetCustomerRequest { CustomerId = 123 });
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

## RPC task

Use `AddTaskRpc(key)` to send an RPC request from the pipeline. The only parameter is the key used to get the outgoing request object from the shared `TaskContext`.

```csharp
var context = new TaskContext();
context.Set("CustomerRequest", new GetCustomerRequest
{
    CustomerId = 123
});

var result = await new TaskPipeline()
    .AddTaskRpc("CustomerRequest")
    .ExecuteAsync(context);
```

The RPC endpoint name is the same as the key. In the example above, the endpoint name is `CustomerRequest`.

The response is stored automatically using the same key plus `Response`.

```csharp
var response = result.Context.Get<object>("CustomerRequestResponse");
```

By default, the RPC connection is read from the `NET_TASK_PIPELINE_RPC_URI` environment variable. If the variable is not set, the local development connection is used.

```bash
NET_TASK_PIPELINE_RPC_URI=amqp://guest:guest@localhost:5672/
```

```csharp
public sealed class GetCustomerRequest
{
    public int CustomerId { get; set; }
}
```

## RPC consumer

The consumer must listen to a queue with the same name used in `AddTaskRpc(key)`.

```csharp
.AddTaskRpc("CustomerRequest")
```

For this call, the consumer must listen to:

```text
CustomerRequest
```

The consumer receives a JSON request, processes it, and publishes a JSON response back to the reply queue sent by the RPC caller.

```csharp
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory
{
    Uri = new Uri(
        Environment.GetEnvironmentVariable("NET_TASK_PIPELINE_RPC_URI")
        ?? "amqp://guest:guest@localhost:5672/")
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

const string queueName = "CustomerRequest";

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: false,
    exclusive: false,
    autoDelete: false,
    arguments: null);

await channel.BasicQosAsync(
    prefetchSize: 0,
    prefetchCount: 1,
    global: false);

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (_, eventArgs) =>
{
    string responseJson;

    try
    {
        var requestJson = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
        var request = JsonSerializer.Deserialize<GetCustomerRequest>(requestJson)
            ?? throw new InvalidOperationException("Invalid request payload.");

        var response = await ProcessCustomerAsync(request);
        responseJson = JsonSerializer.Serialize(response);
    }
    catch (Exception ex)
    {
        responseJson = JsonSerializer.Serialize(new
        {
            error = true,
            message = ex.Message
        });
    }

    var responseBytes = Encoding.UTF8.GetBytes(responseJson);

    var replyProperties = new BasicProperties
    {
        CorrelationId = eventArgs.BasicProperties.CorrelationId,
        ContentType = "application/json"
    };

    await channel.BasicPublishAsync(
        exchange: string.Empty,
        routingKey: eventArgs.BasicProperties.ReplyTo!,
        mandatory: false,
        basicProperties: replyProperties,
        body: responseBytes);

    await channel.BasicAckAsync(
        deliveryTag: eventArgs.DeliveryTag,
        multiple: false);
};

await channel.BasicConsumeAsync(
    queue: queueName,
    autoAck: false,
    consumer: consumer);

Console.WriteLine($"RPC consumer listening on queue '{queueName}'.");
Console.WriteLine("Press ENTER to stop.");
Console.ReadLine();

static Task<GetCustomerResponse> ProcessCustomerAsync(GetCustomerRequest request)
{
    return Task.FromResult(new GetCustomerResponse
    {
        CustomerId = request.CustomerId,
        Name = $"Customer {request.CustomerId}"
    });
}

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

The response must preserve the original correlation id.

```csharp
CorrelationId = eventArgs.BasicProperties.CorrelationId
```

The response must be published to the reply destination received in the request.

```csharp
routingKey: eventArgs.BasicProperties.ReplyTo!
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

## License

MIT
