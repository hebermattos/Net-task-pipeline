using System;

namespace NetTaskPipeline;

/// <summary>
/// Provides key-based RPC registration extensions for <see cref="TaskPipeline"/>.
/// </summary>
public static class TaskRpcKeyExtensions
{
    /// <summary>
    /// Adds an RPC task using only the context key that contains the outgoing request object.
    /// </summary>
    public static TaskPipeline AddTaskRpc(
        this TaskPipeline pipeline,
        string requestKey,
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        if (string.IsNullOrWhiteSpace(requestKey))
            throw new ArgumentException("RPC request key is required.", nameof(requestKey));

        return pipeline.AddTaskRpc<object, object>(
            endpointName: requestKey,
            requestFactory: context => context.Get<object>(requestKey),
            responseKey: $"{requestKey}Response",
            retryCount: retryCount,
            timeout: timeout,
            name: name ?? $"RPC {requestKey}");
    }
}
