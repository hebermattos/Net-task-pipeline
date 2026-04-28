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

        if (serviceProvider == null)
            throw new ArgumentNullException(nameof(serviceProvider));

        return pipeline.RegisterTaskFactory(taskType =>
            (ITask)ActivatorUtilities.GetServiceOrCreateInstance(serviceProvider, taskType));
    }
}
