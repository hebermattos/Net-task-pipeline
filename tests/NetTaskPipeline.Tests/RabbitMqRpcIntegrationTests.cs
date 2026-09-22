using System.Text;
using System.Text.Json;
using NetTaskPipeline;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NetTaskPipeline.Tests;

public sealed class RabbitMqRpcIntegrationTests
{
    private const string ConnectionUri = "amqp://guest:guest@localhost:5672/";

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ExecuteAsync_RoundTripsTypedResponseThroughRabbitMq()
    {
        var queueName = $"net-task-pipeline-test-{Guid.NewGuid():N}";
        await using var server = await RpcTestServer.StartAsync(queueName);

        var context = new TaskContext();
        var result = await new TaskPipeline()
            .AddTaskRpc<TestRequest, TestResponse>(
                _ => new TestRequest { Value = 21 },
                options =>
                {
                    options.ConnectionUri = ConnectionUri;
                    options.RoutingKey = queueName;
                    options.ResponseKey = "Response";
                    options.Timeout = TimeSpan.FromSeconds(5);
                })
            .ExecuteAsync(context);

        Assert.True(result.Success);
        var response = result.Context.Get<TestResponse>("Response");
        Assert.Equal(42, response.Value);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ExecuteAsync_TimesOutWhenNoRpcConsumerResponds()
    {
        var result = await new TaskPipeline()
            .AddTaskRpc<TestRequest, TestResponse>(
                _ => new TestRequest { Value = 1 },
                options =>
                {
                    options.ConnectionUri = ConnectionUri;
                    options.RoutingKey = $"missing-{Guid.NewGuid():N}";
                    options.Timeout = TimeSpan.FromMilliseconds(250);
                })
            .ExecuteAsync();

        Assert.False(result.Success);
        Assert.IsType<TimeoutException>(Assert.Single(result.TaskResults).Exception);
    }

    public sealed class TestRequest { public int Value { get; set; } }
    public sealed class TestResponse { public int Value { get; set; } }

    private sealed class RpcTestServer : IAsyncDisposable
    {
        private readonly IConnection _connection;
        private readonly IChannel _channel;

        private RpcTestServer(IConnection connection, IChannel channel)
        {
            _connection = connection;
            _channel = channel;
        }

        public static async Task<RpcTestServer> StartAsync(string queueName)
        {
            var factory = new ConnectionFactory { Uri = new Uri(ConnectionUri) };
            var connection = await factory.CreateConnectionAsync();
            var channel = await connection.CreateChannelAsync();
            await channel.QueueDeclareAsync(queueName, false, false, true);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, args) =>
            {
                var request = JsonSerializer.Deserialize<TestRequest>(Encoding.UTF8.GetString(args.Body.ToArray()))!;
                var response = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new TestResponse { Value = request.Value * 2 }));
                var properties = new BasicProperties
                {
                    CorrelationId = args.BasicProperties.CorrelationId,
                    ContentType = "application/json"
                };
                await channel.BasicPublishAsync(string.Empty, args.BasicProperties.ReplyTo!, false, properties, response);
                await channel.BasicAckAsync(args.DeliveryTag, false);
            };
            await channel.BasicConsumeAsync(queueName, false, consumer);
            return new RpcTestServer(connection, channel);
        }

        public async ValueTask DisposeAsync()
        {
            await _channel.DisposeAsync();
            await _connection.DisposeAsync();
        }
    }
}
