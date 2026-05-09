using NetTaskPipeline;

Console.WriteLine("Advanced NetTaskPipeline example");
Console.WriteLine();

var context = new TaskContext();
context.Set("CorrelationId", Guid.NewGuid().ToString("N"));

var result = await new TaskPipeline()
    .OnError(ErrorMode.ContinueOnError)
    .WithRetry(1)
    .WithTimeout(TimeSpan.FromSeconds(5))
    .WithMaxDegreeOfParallelism(2)
    .AddTask<LoadOrderTask>(name: "Load order")
    .AddParallel<CalculateTotalsTask, GenerateInvoiceTask, SendNotificationTask>()
    .AddTask<UnstableExternalApiTask>(retryCount: 2, timeout: TimeSpan.FromSeconds(30), name: "External API with retry")
    .AddTask<TimeoutTask>(timeout: TimeSpan.FromSeconds(5), name: "Task with timeout")
    .AddTask<FinalizeOrderTask>(name: "Finalize order")
    .ExecuteAsync(context);

Console.WriteLine();
Console.WriteLine("Execution summary");
Console.WriteLine($"Pipeline success: {result.Success}");
Console.WriteLine($"Total duration: {result.Duration.TotalMilliseconds:N0} ms");
Console.WriteLine($"Correlation ID: {result.Context.Get<string>("CorrelationId")}");
Console.WriteLine();

foreach (var taskResult in result.TaskResults)
{
    Console.WriteLine($"Task: {taskResult.TaskName}");
    Console.WriteLine($"  Group: {taskResult.GroupIndex}");
    Console.WriteLine($"  Status: {taskResult.Status}");
    Console.WriteLine($"  Attempts: {taskResult.Attempts}");
    Console.WriteLine($"  Duration: {taskResult.Duration.TotalMilliseconds:N0} ms");

    if (taskResult.Exception is not null)
        Console.WriteLine($"  Error: {taskResult.Exception.GetType().Name} - {taskResult.Exception.Message}");

    Console.WriteLine();
}

public sealed class LoadOrderTask : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        await Task.Delay(500, cancellationToken);

        context.Set("OrderId", 1001);
        context.Set("CustomerEmail", "customer@example.com");
        context.Set("Subtotal", 250.00m);

        Console.WriteLine("Order loaded.");
    }
}

public sealed class CalculateTotalsTask : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        var subtotal = context.Get<decimal>("Subtotal");

        await Task.Delay(700, cancellationToken);

        var tax = subtotal * 0.10m;
        var total = subtotal + tax;

        context.Set("Tax", tax);
        context.Set("Total", total);

        Console.WriteLine($"Totals calculated. Total: {total:C}");
    }
}

public sealed class GenerateInvoiceTask : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        var orderId = context.Get<int>("OrderId");

        await Task.Delay(900, cancellationToken);

        var invoiceNumber = $"INV-{orderId}-{DateTime.UtcNow:yyyyMMddHHmmss}";
        context.Set("InvoiceNumber", invoiceNumber);

        Console.WriteLine($"Invoice generated: {invoiceNumber}");
    }
}

public sealed class SendNotificationTask : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        var email = context.Get<string>("CustomerEmail");

        await Task.Delay(600, cancellationToken);

        Console.WriteLine($"Notification sent to {email}.");
    }
}

public sealed class UnstableExternalApiTask : ITask
{
    private int _attempts;

    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        _attempts++;

        await Task.Delay(400, cancellationToken);

        if (_attempts < 3)
            throw new InvalidOperationException("Temporary external API failure.");

        context.Set("ExternalConfirmationCode", "EXT-OK-123");

        Console.WriteLine("External API call completed after retry.");
    }
}

public sealed class TimeoutTask : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine("long task");

        await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
    }
}

public sealed class FinalizeOrderTask : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        var orderId = context.Get<int>("OrderId");
        var total = context.TryGet<decimal>("Total", out var value) ? value : 0m;
        var confirmation = context.TryGet<string>("ExternalConfirmationCode", out var code) ? code : "not available";

        await Task.Delay(500, cancellationToken);

        Console.WriteLine($"Order {orderId} finalized. Total: {total:C}. External confirmation: {confirmation}.");
    }
}
