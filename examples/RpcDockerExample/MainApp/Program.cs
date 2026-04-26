using System.Text.Json;
using NetTaskPipeline;

Console.WriteLine("RPC Docker example - main app");
Console.WriteLine();

var context = new TaskContext();

context.Set(RpcQueues.CustomerRequest, new GetCustomerRequest
{
    CustomerId = 123
});

context.Set(RpcQueues.OrderRequest, new GetOrderRequest
{
    OrderId = 987,
    CustomerId = 123
});

var result = await new TaskPipeline()
    .WithTimeout(TimeSpan.FromSeconds(20))
    .AddTaskRpc<GetCustomerRequest, GetCustomerResponse>(RpcQueues.CustomerRequest)
    .AddTaskRpc<GetOrderRequest, GetOrderResponse>(RpcQueues.OrderRequest)
    .ExecuteAsync(context);

Console.WriteLine($"Pipeline success: {result.Success}");
Console.WriteLine($"Total duration: {result.Duration.TotalMilliseconds:N0} ms");
Console.WriteLine();

foreach (var taskResult in result.TaskResults)
{
    Console.WriteLine($"- {taskResult.TaskName}: {taskResult.Status} ({taskResult.Duration.TotalMilliseconds:N0} ms)");

    if (taskResult.Exception != null)
        Console.WriteLine($"  Error: {taskResult.Exception.Message}");
}

Console.WriteLine();

if (result.Success)
{
    var jsonOptions = new JsonSerializerOptions
    {
        WriteIndented = true
    };

    var customerResponse = result.Context.Get<GetCustomerResponse>($"{RpcQueues.CustomerRequest}Response");
    var orderResponse = result.Context.Get<GetOrderResponse>($"{RpcQueues.OrderRequest}Response");

    Console.WriteLine("Customer RPC response:");
    Console.WriteLine(JsonSerializer.Serialize(customerResponse, jsonOptions));
    Console.WriteLine();

    Console.WriteLine("Order RPC response:");
    Console.WriteLine(JsonSerializer.Serialize(orderResponse, jsonOptions));
}

public static class RpcQueues
{
    public const string CustomerRequest = "CustomerRequest";
    public const string OrderRequest = "OrderRequest";
}

public sealed class GetCustomerRequest
{
    public int CustomerId { get; set; }
}

public sealed class GetCustomerResponse
{
    public int CustomerId { get; set; }

    public string Name { get; set; } = string.Empty;

    public string Status { get; set; } = string.Empty;
}

public sealed class GetOrderRequest
{
    public int OrderId { get; set; }

    public int CustomerId { get; set; }
}

public sealed class GetOrderResponse
{
    public int OrderId { get; set; }

    public int CustomerId { get; set; }

    public decimal Total { get; set; }

    public bool Approved { get; set; }
}
