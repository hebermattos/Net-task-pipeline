using System;
using System.Threading;
using System.Threading.Tasks;

namespace NetTaskPipeline;

/// <summary>
/// Provides RabbitMQ RPC registration extensions for <see cref="TaskPipeline"/>.
/// </summary>
public static class RabbitMqRpcExtensions
{
    /// <summary>
    /// Adds a RabbitMQ RPC task to the pipeline.
    /// </summary>
    public static TaskPipeline AddTaskRpc<TRequest, TResponse>(
        this TaskPipeline pipeline,
        Func<TaskContext, TRequest> requestFactory,
        Action<RabbitMqRpcOptions> configure,
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
    {
        if (requestFactory == null)
            throw new ArgumentNullException(nameof(requestFactory));

        return pipeline.AddTaskRpc<TRequest, TResponse>(
            (context, _) => Task.FromResult(requestFactory(context)),
            configure,
            retryCount,
            timeout,
            name);
    }

    /// <summary>
    /// Adds an asynchronous RabbitMQ RPC task to the pipeline.
    /// </summary>
    public static TaskPipeline AddTaskRpc<TRequest, TResponse>(
        this TaskPipeline pipeline,
        Func<TaskContext, CancellationToken, Task<TRequest>> requestFactory,
        Action<RabbitMqRpcOptions> configure,
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        if (requestFactory == null)
            throw new ArgumentNullException(nameof(requestFactory));

        if (configure == null)
            throw new ArgumentNullException(nameof(configure));

        var options = new RabbitMqRpcOptions();
        configure(options);

        return pipeline.AddTask(
            new RabbitMqRpcTask<TRequest, TResponse>(requestFactory, options),
            retryCount,
            timeout,
            name ?? "RabbitMQ RPC");
    }
}
