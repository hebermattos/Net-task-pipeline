using System;
using System.Text.Json;

namespace NetTaskPipeline;

/// <summary>
/// Defines optional RPC settings used by <see cref="RabbitMqRpcExtensions"/>.
/// </summary>
public sealed class TaskRpcOptions
{
    /// <summary>
    /// Gets or sets the broker connection URI.
    /// If not provided, the value is read from NET_TASK_PIPELINE_RPC_URI or falls back to localhost.
    /// </summary>
    public string ConnectionUri { get; set; } =
        Environment.GetEnvironmentVariable("NET_TASK_PIPELINE_RPC_URI")
        ?? "amqp://guest:guest@localhost:5672/";

    /// <summary>
    /// Gets or sets the maximum time to wait for the RPC response.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets whether the request message should be marked as persistent.
    /// </summary>
    public bool Persistent { get; set; }

    /// <summary>
    /// Gets or sets the JSON serializer options used for request and response payloads.
    /// </summary>
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true
    };
}
