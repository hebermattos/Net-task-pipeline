using System;

namespace NetTaskPipeline;

/// <summary>
/// Provides key-based RPC registration extensions for <see cref="TaskPipeline"/>.
/// </summary>
public static class TaskRpcKeyExtensions
{
    /// <summary>
    /// Adds an RPC task using the context key that contains the outgoing request object.
    /// The response is deserialized as <typeparamref name="TResponse"/> and stored as {requestKey}Response.
    /// </summary>
    public static TaskPipeline AddTaskRpc<TRequest, TResponse>(
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

        return pipeline.AddTaskRpc<TRequest, TResponse>(
            endpointName: requestKey,
            requestFactory: context => context.Get<TRequest>(requestKey),
            responseKey: $"{requestKey}Response",
            retryCount: retryCount,
            timeout: timeout,
            name: name ?? $"RPC {requestKey}");
    }
}
