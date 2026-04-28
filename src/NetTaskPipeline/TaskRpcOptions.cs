using System;
using System.Text.Json;

namespace NetTaskPipeline;

/// <summary>
/// Defines optional RabbitMQ RPC settings used by the RabbitMQ RPC task extensions.
/// </summary>
public sealed class TaskRpcOptions
{
    /// <summary>
    /// Gets or sets the RabbitMQ broker connection URI.
    /// If not provided, the value is read from NET_TASK_PIPELINE_RPC_URI or falls back to localhost.
    /// </summary>
    public string ConnectionUri { get; set; } =
        Environment.GetEnvironmentVariable("NET_TASK_PIPELINE_RPC_URI")
        ?? "amqp://guest:guest@localhost:5672/";

    /// <summary>
    /// Gets or sets the maximum time to wait for the RabbitMQ RPC response.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets whether the RabbitMQ request message should be marked as persistent.
    /// </summary>
    public bool Persistent { get; set; }

    /// <summary>
    /// Gets or sets the JSON serializer options used for RabbitMQ RPC request and response payloads.
    /// </summary>
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true
    };
}
