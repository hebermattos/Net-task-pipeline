using System;
using System.Threading;
using System.Threading.Tasks;

namespace NetTaskPipeline;

/// <summary>
/// Provides HTTP task registration extensions for <see cref="TaskPipeline"/>.
/// </summary>
public static class HttpTaskExtensions
{
    /// <summary>
    /// Adds an HTTP task to the pipeline.
    /// </summary>
    public static TaskPipeline AddTaskHttp<TRequest, TResponse>(
        this TaskPipeline pipeline,
        Func<TaskContext, TRequest> requestFactory,
        Action<HttpTaskOptions> configure,
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
    {
        if (requestFactory == null)
            throw new ArgumentNullException(nameof(requestFactory));

        return pipeline.AddTaskHttp<TRequest, TResponse>(
            (context, _) => Task.FromResult(requestFactory(context)),
            configure,
            retryCount,
            timeout,
            name);
    }

    /// <summary>
    /// Adds an asynchronous HTTP task to the pipeline.
    /// </summary>
    public static TaskPipeline AddTaskHttp<TRequest, TResponse>(
        this TaskPipeline pipeline,
        Func<TaskContext, CancellationToken, Task<TRequest>> requestFactory,
        Action<HttpTaskOptions> configure,
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

        var options = new HttpTaskOptions();
        configure(options);

        return pipeline.AddTask(
            new HttpTask<TRequest, TResponse>(requestFactory, options),
            retryCount,
            timeout,
            name ?? "HTTP");
    }
}
