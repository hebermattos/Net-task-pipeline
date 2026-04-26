using System;
using System.Text.Json;

namespace NetTaskPipeline;

/// <summary>
/// Defines the RabbitMQ RPC options used by <see cref="RabbitMqRpcTask{TRequest,TResponse}"/>.
/// </summary>
public sealed class RabbitMqRpcOptions
{
    /// <summary>
    /// Gets or sets the RabbitMQ connection URI.
    /// </summary>
    public string ConnectionUri { get; set; } = "amqp://guest:guest@localhost:5672/";

    /// <summary>
    /// Gets or sets the exchange used to publish the RPC request.
    /// Empty means the default exchange.
    /// </summary>
    public string Exchange { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the routing key used to publish the RPC request.
    /// For the default exchange, this is usually the target queue name.
    /// </summary>
    public string RoutingKey { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets whether the request message is mandatory.
    /// </summary>
    public bool Mandatory { get; set; }

    /// <summary>
    /// Gets or sets whether the request message should be marked as persistent.
    /// </summary>
    public bool Persistent { get; set; }

    /// <summary>
    /// Gets or sets the request content type.
    /// </summary>
    public string ContentType { get; set; } = "application/json";

    /// <summary>
    /// Gets or sets the optional client-provided connection name shown in RabbitMQ management UI.
    /// </summary>
    public string? ClientProvidedName { get; set; } = "NetTaskPipeline RPC";

    /// <summary>
    /// Gets or sets the maximum time to wait for the RPC response.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the context key used to store the RPC response.
    /// </summary>
    public string ResponseKey { get; set; } = "RpcResponse";

    /// <summary>
    /// Gets or sets the JSON serializer options used for request and response payloads.
    /// </summary>
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true
    };
}
