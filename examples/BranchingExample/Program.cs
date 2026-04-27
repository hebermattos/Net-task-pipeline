using NetTaskPipeline;

Console.WriteLine("NetTaskPipeline branching example");
Console.WriteLine();

var context = new TaskContext();
context.Set("OrderTotal", 1_250m);
context.Set("CustomerType", "premium");

var result = await new TaskPipeline()
    .AddTask<LoadOrderTask>()
    .AddBranch(
        selector: ctx => ctx.Get<string>("CustomerType"),
        configure: branch => branch
            .When("premium", flow => flow
                .AddTask<RequireManagerApprovalTask>()
                .AddTask<ApplyPremiumDiscountTask>())
            .When("standard", flow => flow
                .AddTask<RequireManagerApprovalTask>())
            .Default(flow => flow
                .AddTask<AutoApproveOrderTask>()),
        name: "Customer type decision")
    .AddTask<SaveOrderTask>()
    .ExecuteAsync(context);

Console.WriteLine();
Console.WriteLine($"Pipeline success: {result.Success}");
Console.WriteLine($"Approval status: {result.Context.Get<string>("ApprovalStatus")}");
Console.WriteLine($"Discount applied: {result.Context.Get<bool>("DiscountApplied")}");
Console.WriteLine();

foreach (var taskResult in result.TaskResults)
{
    Console.WriteLine($"- {taskResult.TaskName}: {taskResult.Status} ({taskResult.Duration.TotalMilliseconds:N0} ms)");
}

public sealed class LoadOrderTask : ITask
{
    public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        var orderTotal = context.Get<decimal>("OrderTotal");
        var customerType = context.Get<string>("CustomerType");

        Console.WriteLine($"Loaded {customerType} order with total {orderTotal:C}.");

        return Task.CompletedTask;
    }
}

public sealed class RequireManagerApprovalTask : ITask
{
    public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Order requires manager approval.");
        context.Set("ApprovalStatus", "Manager approval required");

        return Task.CompletedTask;
    }
}

public sealed class ApplyPremiumDiscountTask : ITask
{
    public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        var customerType = context.Get<string>("CustomerType");
        var discountApplied = string.Equals(customerType, "premium", StringComparison.OrdinalIgnoreCase);

        Console.WriteLine(discountApplied
            ? "Premium discount applied."
            : "No premium discount applied.");

        context.Set("DiscountApplied", discountApplied);

        return Task.CompletedTask;
    }
}

public sealed class AutoApproveOrderTask : ITask
{
    public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Order auto-approved by default branch.");
        context.Set("ApprovalStatus", "Auto-approved");
        context.Set("DiscountApplied", false);

        return Task.CompletedTask;
    }
}

public sealed class SaveOrderTask : ITask
{
    public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Order saved.");
        return Task.CompletedTask;
    }
}
