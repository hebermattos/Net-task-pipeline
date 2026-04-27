using System;

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
}
