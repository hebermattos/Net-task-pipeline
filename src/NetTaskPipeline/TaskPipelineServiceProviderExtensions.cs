using System;
using Microsoft.Extensions.DependencyInjection;

namespace NetTaskPipeline;

public static class TaskPipelineServiceProviderExtensions
{
    /// <summary>
    /// Registers the service provider used by generic task registration methods.
    /// </summary>
    public static TaskPipeline WithServiceProvider(
        this TaskPipeline pipeline,
        IServiceProvider serviceProvider)
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        return pipeline.RegisterServiceProvider(serviceProvider);
    }

    public static TaskPipeline AddTask<TTask>(
        this TaskPipeline pipeline,
        IServiceProvider serviceProvider,
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
        where TTask : ITask
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        if (serviceProvider == null)
            throw new ArgumentNullException(nameof(serviceProvider));

        return pipeline.AddTask(
            ActivatorUtilities.GetServiceOrCreateInstance<TTask>(serviceProvider),
            retryCount,
            timeout,
            name ?? typeof(TTask).Name);
    }

    public static TaskPipeline AddParallel<TTask1, TTask2>(
        this TaskPipeline pipeline,
        IServiceProvider serviceProvider,
        int? retryCount = null,
        TimeSpan? timeout = null)
        where TTask1 : ITask
        where TTask2 : ITask
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        if (serviceProvider == null)
            throw new ArgumentNullException(nameof(serviceProvider));

        return pipeline.AddParallel(
            new ITask[]
            {
                ActivatorUtilities.GetServiceOrCreateInstance<TTask1>(serviceProvider),
                ActivatorUtilities.GetServiceOrCreateInstance<TTask2>(serviceProvider)
            },
            retryCount,
            timeout);
    }
}
