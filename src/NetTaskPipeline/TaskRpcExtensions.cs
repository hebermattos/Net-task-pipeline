using System;
using System.Threading;
using System.Threading.Tasks;

namespace NetTaskPipeline;

/// <summary>
/// Provides simplified RPC registration extensions for <see cref="TaskPipeline"/>.
/// </summary>
public static class TaskRpcExtensions
{
    /// <summary>
    /// Gets the default context key used to store the RPC response for a request type.
    /// </summary>
    public static string GetResponseKey<TRequest>()
    {
        return $"{typeof(TRequest).Name}Response";
    }

    /// <summary>
    /// Adds an RPC task using only the request object factory. The endpoint name is inferred from the request type name.
    /// </summary>
    public static TaskPipeline AddTaskRpc<TRequest>(
        this TaskPipeline pipeline,
        Func<TaskContext, TRequest> requestFactory,
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
    {
        if (requestFactory == null)
            throw new ArgumentNullException(nameof(requestFactory));

        return pipeline.AddTaskRpc<TRequest>(
            (context, _) => Task.FromResult(requestFactory(context)),
            retryCount,
            timeout,
            name);
    }

    /// <summary>
    /// Adds an asynchronous RPC task using only the request object factory. The endpoint name is inferred from the request type name.
    /// </summary>
    public static TaskPipeline AddTaskRpc<TRequest>(
        this TaskPipeline pipeline,
        Func<TaskContext, CancellationToken, Task<TRequest>> requestFactory,
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        if (requestFactory == null)
            throw new ArgumentNullException(nameof(requestFactory));

        var endpointName = typeof(TRequest).Name;
        var responseKey = GetResponseKey<TRequest>();

        return pipeline.AddTaskRpc<TRequest, object>(
            endpointName,
            requestFactory,
            responseKey,
            configure: null,
            retryCount,
            timeout,
            name ?? $"RPC {endpointName}");
    }

    /// <summary>
    /// Adds an RPC task using a simple endpoint name and the default broker connection.
    /// </summary>
    public static TaskPipeline AddTaskRpc<TRequest, TResponse>(
        this TaskPipeline pipeline,
        string endpointName,
        Func<TaskContext, TRequest> requestFactory,
        string responseKey = "RpcResponse",
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
    {
        if (requestFactory == null)
            throw new ArgumentNullException(nameof(requestFactory));

        return pipeline.AddTaskRpc<TRequest, TResponse>(
            endpointName,
            (context, _) => Task.FromResult(requestFactory(context)),
            responseKey,
            configure: null,
            retryCount,
            timeout,
            name);
    }

    /// <summary>
    /// Adds an asynchronous RPC task using a simple endpoint name and the default broker connection.
    /// </summary>
    public static TaskPipeline AddTaskRpc<TRequest, TResponse>(
        this TaskPipeline pipeline,
        string endpointName,
        Func<TaskContext, CancellationToken, Task<TRequest>> requestFactory,
        string responseKey = "RpcResponse",
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
    {
        return pipeline.AddTaskRpc<TRequest, TResponse>(
            endpointName,
            requestFactory,
            responseKey,
            configure: null,
            retryCount,
            timeout,
            name);
    }

    /// <summary>
    /// Adds an RPC task using a simple endpoint name and optional RPC settings.
    /// </summary>
    public static TaskPipeline AddTaskRpc<TRequest, TResponse>(
        this TaskPipeline pipeline,
        string endpointName,
        Func<TaskContext, TRequest> requestFactory,
        string responseKey,
        Action<TaskRpcOptions>? configure,
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
    {
        if (requestFactory == null)
            throw new ArgumentNullException(nameof(requestFactory));

        return pipeline.AddTaskRpc<TRequest, TResponse>(
            endpointName,
            (context, _) => Task.FromResult(requestFactory(context)),
            responseKey,
            configure,
            retryCount,
            timeout,
            name);
    }

    /// <summary>
    /// Adds an asynchronous RPC task using a simple endpoint name and optional RPC settings.
    /// </summary>
    public static TaskPipeline AddTaskRpc<TRequest, TResponse>(
        this TaskPipeline pipeline,
        string endpointName,
        Func<TaskContext, CancellationToken, Task<TRequest>> requestFactory,
        string responseKey,
        Action<TaskRpcOptions>? configure,
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        if (string.IsNullOrWhiteSpace(endpointName))
            throw new ArgumentException("RPC endpoint name is required.", nameof(endpointName));

        if (requestFactory == null)
            throw new ArgumentNullException(nameof(requestFactory));

        if (string.IsNullOrWhiteSpace(responseKey))
            throw new ArgumentException("RPC response key is required.", nameof(responseKey));

        var options = new TaskRpcOptions();
        configure?.Invoke(options);

        var transportOptions = new RabbitMqRpcOptions
        {
            ConnectionUri = options.ConnectionUri,
            RoutingKey = endpointName,
            ResponseKey = responseKey,
            Timeout = options.Timeout,
            Persistent = options.Persistent,
            JsonSerializerOptions = options.JsonSerializerOptions
        };

        return pipeline.AddTask(
            new RabbitMqRpcTask<TRequest, TResponse>(requestFactory, transportOptions),
            retryCount,
            timeout,
            name ?? $"RPC {endpointName}");
    }
}
