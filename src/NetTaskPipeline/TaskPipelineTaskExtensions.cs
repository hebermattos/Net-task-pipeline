using System;

namespace NetTaskPipeline;

/// <summary>
/// Provides generic task registration extensions for <see cref="TaskPipeline"/>.
/// </summary>
public static class TaskPipelineTaskExtensions
{
    /// <summary>
    /// Adds a single sequential task by type.
    /// </summary>
    public static TaskPipeline AddTask<TTask>(
        this TaskPipeline pipeline,
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
        where TTask : ITask
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        return pipeline.AddTask(
            pipeline.CreateTask<TTask>(),
            retryCount,
            timeout,
            name ?? typeof(TTask).Name);
    }

    /// <summary>
    /// Adds a parallel task group with two tasks by type.
    /// </summary>
    public static TaskPipeline AddParallel<TTask1, TTask2>(
        this TaskPipeline pipeline,
        int? retryCount = null,
        TimeSpan? timeout = null)
        where TTask1 : ITask
        where TTask2 : ITask
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        return pipeline.AddParallel(
            new ITask[]
            {
                pipeline.CreateTask<TTask1>(),
                pipeline.CreateTask<TTask2>()
            },
            retryCount,
            timeout);
    }

    /// <summary>
    /// Adds a parallel task group with three tasks by type.
    /// </summary>
    public static TaskPipeline AddParallel<TTask1, TTask2, TTask3>(
        this TaskPipeline pipeline,
        int? retryCount = null,
        TimeSpan? timeout = null)
        where TTask1 : ITask
        where TTask2 : ITask
        where TTask3 : ITask
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        return pipeline.AddParallel(
            new ITask[]
            {
                pipeline.CreateTask<TTask1>(),
                pipeline.CreateTask<TTask2>(),
                pipeline.CreateTask<TTask3>()
            },
            retryCount,
            timeout);
    }

    /// <summary>
    /// Adds a parallel task group with four tasks by type.
    /// </summary>
    public static TaskPipeline AddParallel<TTask1, TTask2, TTask3, TTask4>(
        this TaskPipeline pipeline,
        int? retryCount = null,
        TimeSpan? timeout = null)
        where TTask1 : ITask
        where TTask2 : ITask
        where TTask3 : ITask
        where TTask4 : ITask
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        return pipeline.AddParallel(
            new ITask[]
            {
                pipeline.CreateTask<TTask1>(),
                pipeline.CreateTask<TTask2>(),
                pipeline.CreateTask<TTask3>(),
                pipeline.CreateTask<TTask4>()
            },
            retryCount,
            timeout);
    }
}
