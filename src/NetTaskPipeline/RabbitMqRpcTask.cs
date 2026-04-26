using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NetTaskPipeline;

/// <summary>
/// Executes a RabbitMQ RPC request and stores the response in the shared pipeline context.
/// </summary>
public sealed class RabbitMqRpcTask<TRequest, TResponse> : ITask
{
    private readonly Func<TaskContext, CancellationToken, Task<TRequest>> _requestFactory;
    private readonly RabbitMqRpcOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqRpcTask{TRequest,TResponse}"/> class.
    /// </summary>
    public RabbitMqRpcTask(
        Func<TaskContext, CancellationToken, Task<TRequest>> requestFactory,
        RabbitMqRpcOptions options)
    {
        _requestFactory = requestFactory ?? throw new ArgumentNullException(nameof(requestFactory));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        ValidateOptions(_options);

        using var timeoutCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCancellationTokenSource.CancelAfter(_options.Timeout);
        var timeoutCancellationToken = timeoutCancellationTokenSource.Token;

        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.ConnectionUri)
        };

        if (!string.IsNullOrWhiteSpace(_options.ClientProvidedName))
            factory.ClientProvidedName = _options.ClientProvidedName;

        await using var connection = await factory.CreateConnectionAsync(timeoutCancellationToken).ConfigureAwait(false);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: timeoutCancellationToken).ConfigureAwait(false);

        var replyQueue = await channel.QueueDeclareAsync(
            queue: string.Empty,
            durable: false,
            exclusive: true,
            autoDelete: true,
            arguments: null,
            cancellationToken: timeoutCancellationToken).ConfigureAwait(false);

        var replyQueueName = replyQueue.QueueName;
        var correlationId = Guid.NewGuid().ToString("N");
        var responseCompletionSource = new TaskCompletionSource<TResponse>(TaskCreationOptions.RunContinuationsAsynchronously);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, eventArgs) =>
        {
            if (!string.Equals(eventArgs.BasicProperties.CorrelationId, correlationId, StringComparison.Ordinal))
                return Task.CompletedTask;

            try
            {
                var responseBody = eventArgs.Body.ToArray();
                var responseJson = Encoding.UTF8.GetString(responseBody);
                var response = JsonSerializer.Deserialize<TResponse>(responseJson, _options.JsonSerializerOptions);

                if (response == null)
                {
                    responseCompletionSource.TrySetException(
                        new InvalidOperationException("The RabbitMQ RPC response could not be deserialized."));
                }
                else
                {
                    responseCompletionSource.TrySetResult(response);
                }
            }
            catch (Exception ex)
            {
                responseCompletionSource.TrySetException(ex);
            }

            return Task.CompletedTask;
        };

        var consumerTag = await channel.BasicConsumeAsync(
            queue: replyQueueName,
            autoAck: true,
            consumer: consumer,
            cancellationToken: timeoutCancellationToken).ConfigureAwait(false);

        try
        {
            var request = await _requestFactory(context, timeoutCancellationToken).ConfigureAwait(false);
            var requestJson = JsonSerializer.Serialize(request, _options.JsonSerializerOptions);
            var requestBody = Encoding.UTF8.GetBytes(requestJson);

            var properties = new BasicProperties
            {
                CorrelationId = correlationId,
                ReplyTo = replyQueueName,
                ContentType = _options.ContentType,
                Persistent = _options.Persistent
            };

            await channel.BasicPublishAsync(
                exchange: _options.Exchange,
                routingKey: _options.RoutingKey,
                mandatory: _options.Mandatory,
                basicProperties: properties,
                body: requestBody,
                cancellationToken: timeoutCancellationToken).ConfigureAwait(false);

            var completedTask = await Task.WhenAny(
                responseCompletionSource.Task,
                Task.Delay(_options.Timeout, timeoutCancellationToken)).ConfigureAwait(false);

            if (completedTask != responseCompletionSource.Task)
                throw new TimeoutException($"The RabbitMQ RPC call timed out after {_options.Timeout}.");

            var response = await responseCompletionSource.Task.ConfigureAwait(false);
            context.Set(_options.ResponseKey, response);
        }
        finally
        {
            await channel.BasicCancelAsync(consumerTag, cancellationToken: CancellationToken.None).ConfigureAwait(false);
        }
    }

    private static void ValidateOptions(RabbitMqRpcOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionUri))
            throw new ArgumentException("RabbitMQ connection URI is required.", nameof(options));

        if (string.IsNullOrWhiteSpace(options.RoutingKey))
            throw new ArgumentException("RabbitMQ routing key is required.", nameof(options));

        if (options.Timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "RabbitMQ RPC timeout must be greater than zero.");

        if (string.IsNullOrWhiteSpace(options.ResponseKey))
            throw new ArgumentException("RabbitMQ RPC response key is required.", nameof(options));
    }
}
