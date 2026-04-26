using System;
using System.Linq;

namespace NetTaskPipeline;

public static class TaskPipelineServiceProviderExtensions
{
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
            CreateTask<TTask>(serviceProvider),
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
                CreateTask<TTask1>(serviceProvider),
                CreateTask<TTask2>(serviceProvider)
            },
            retryCount,
            timeout);
    }

    private static TTask CreateTask<TTask>(IServiceProvider serviceProvider)
        where TTask : ITask
    {
        var taskType = typeof(TTask);
        var registeredTask = serviceProvider.GetService(taskType);

        if (registeredTask != null)
            return (TTask)registeredTask;

        foreach (var constructor in taskType.GetConstructors().OrderByDescending(ctor => ctor.GetParameters().Length))
        {
            var parameters = constructor.GetParameters();
            var arguments = new object?[parameters.Length];
            var canUseConstructor = true;

            for (var index = 0; index < parameters.Length; index++)
            {
                var service = serviceProvider.GetService(parameters[index].ParameterType);

                if (service == null)
                {
                    canUseConstructor = false;
                    break;
                }

                arguments[index] = service;
            }

            if (canUseConstructor)
                return (TTask)constructor.Invoke(arguments);
        }

        throw new InvalidOperationException($"Unable to create task '{taskType.FullName}'.");
    }
}
