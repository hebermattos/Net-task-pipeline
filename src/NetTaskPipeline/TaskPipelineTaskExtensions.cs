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
        where TTask : ITask, new()
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        return pipeline.AddTask(
            new TTask(),
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
        where TTask1 : ITask, new()
        where TTask2 : ITask, new()
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        return pipeline.AddParallel(
            new ITask[]
            {
                new TTask1(),
                new TTask2()
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
        where TTask1 : ITask, new()
        where TTask2 : ITask, new()
        where TTask3 : ITask, new()
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        return pipeline.AddParallel(
            new ITask[]
            {
                new TTask1(),
                new TTask2(),
                new TTask3()
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
        where TTask1 : ITask, new()
        where TTask2 : ITask, new()
        where TTask3 : ITask, new()
        where TTask4 : ITask, new()
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        return pipeline.AddParallel(
            new ITask[]
            {
                new TTask1(),
                new TTask2(),
                new TTask3(),
                new TTask4()
            },
            retryCount,
            timeout);
    }
}
