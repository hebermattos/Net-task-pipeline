using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

Console.WriteLine("RPC Docker example - consumer");

var connectionUri = Environment.GetEnvironmentVariable("NET_TASK_PIPELINE_RPC_URI")
    ?? "amqp://guest:guest@localhost:5672/";

var readyFilePath = Environment.GetEnvironmentVariable("RPC_CONSUMER_READY_FILE");
DeleteReadinessFile(readyFilePath);

var factory = new ConnectionFactory
{
    Uri = new Uri(connectionUri),
    ClientProvidedName = "NetTaskPipeline RPC Consumer"
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await RegisterRpcHandlerAsync<GetCustomerRequest, GetCustomerResponse>(
    channel,
    RpcQueues.CustomerRequest,
    ProcessCustomerAsync);

await RegisterRpcHandlerAsync<GetOrderRequest, GetOrderResponse>(
    channel,
    RpcQueues.OrderRequest,
    ProcessOrderAsync);

WriteReadinessFile(readyFilePath);

Console.WriteLine("Consumer ready.");
Console.WriteLine($"Listening on '{RpcQueues.CustomerRequest}' and '{RpcQueues.OrderRequest}'.");

await Task.Delay(Timeout.InfiniteTimeSpan);

static async Task RegisterRpcHandlerAsync<TRequest, TResponse>(
    IChannel channel,
    string queueName,
    Func<TRequest, Task<TResponse>> handler)
{
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
            
            var request = JsonSerializer.Deserialize<TRequest>(requestJson)
                ?? throw new InvalidOperationException("Invalid request payload.");

            var response = await handler(request);
            responseJson = JsonSerializer.Serialize(response);
        }
        catch (Exception ex)
        {
            responseJson = JsonSerializer.Serialize(new RpcErrorResponse
            {
                Error = true,
                Message = ex.Message
            });
        }

        var replyTo = eventArgs.BasicProperties.ReplyTo;

        if (string.IsNullOrWhiteSpace(replyTo))
        {
            await channel.BasicNackAsync(
                deliveryTag: eventArgs.DeliveryTag,
                multiple: false,
                requeue: false);

            return;
        }

        var replyProperties = new BasicProperties
        {
            CorrelationId = eventArgs.BasicProperties.CorrelationId,
            ContentType = "application/json"
        };

        var responseBytes = Encoding.UTF8.GetBytes(responseJson);

        await channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: replyTo,
            mandatory: false,
            basicProperties: replyProperties,
            body: responseBytes);

        await channel.BasicAckAsync(
            deliveryTag: eventArgs.DeliveryTag,
            multiple: false);

        Console.WriteLine($"Processed RPC queue '{queueName}' with correlation id '{eventArgs.BasicProperties.CorrelationId}'.");
    };

    await channel.BasicConsumeAsync(
        queue: queueName,
        autoAck: false,
        consumer: consumer);
}

static void DeleteReadinessFile(string? readyFilePath)
{
    if (string.IsNullOrWhiteSpace(readyFilePath))
        return;

    if (File.Exists(readyFilePath))
        File.Delete(readyFilePath);
}

static void WriteReadinessFile(string? readyFilePath)
{
    if (string.IsNullOrWhiteSpace(readyFilePath))
        return;

    var directory = Path.GetDirectoryName(readyFilePath);

    if (!string.IsNullOrWhiteSpace(directory))
        Directory.CreateDirectory(directory);

    File.WriteAllText(readyFilePath, DateTimeOffset.UtcNow.ToString("O"));
    Console.WriteLine($"Readiness file created at '{readyFilePath}'.");
}

static Task<GetCustomerResponse> ProcessCustomerAsync(GetCustomerRequest request)
{
    return Task.FromResult(new GetCustomerResponse
    {
        CustomerId = request.CustomerId,
        Name = $"Customer {request.CustomerId}",
        Status = "Active"
    });
}

static Task<GetOrderResponse> ProcessOrderAsync(GetOrderRequest request)
{
    return Task.FromResult(new GetOrderResponse
    {
        OrderId = request.OrderId,
        CustomerId = request.CustomerId,
        Total = 199.90m,
        Approved = true
    });
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

public sealed class RpcErrorResponse
{
    public bool Error { get; set; }

    public string Message { get; set; } = string.Empty;
}
