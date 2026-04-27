using System;
using System.Threading;
using System.Threading.Tasks;

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
    /// Adds a single sequential inline task using a delegate.
    /// </summary>
    public static TaskPipeline AddTask(
        this TaskPipeline pipeline,
        string name,
        Func<TaskContext, Task> execute,
        int? retryCount = null,
        TimeSpan? timeout = null)
    {
        if (execute == null)
            throw new ArgumentNullException(nameof(execute));

        return pipeline.AddTask(
            name,
            (context, _) => execute(context),
            retryCount,
            timeout);
    }

    /// <summary>
    /// Adds a single sequential inline task using a delegate with cancellation support.
    /// </summary>
    public static TaskPipeline AddTask(
        this TaskPipeline pipeline,
        string name,
        Func<TaskContext, CancellationToken, Task> execute,
        int? retryCount = null,
        TimeSpan? timeout = null)
    {
        if (pipeline == null)
            throw new ArgumentNullException(nameof(pipeline));

        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("The task name cannot be null, empty, or whitespace.", nameof(name));

        if (execute == null)
            throw new ArgumentNullException(nameof(execute));

        return pipeline.AddTask(
            new InlineTask(execute),
            retryCount,
            timeout,
            name);
    }

    /// <summary>
    /// Adds a single sequential inline task using a delegate and the default task name.
    /// </summary>
    public static TaskPipeline AddTask(
        this TaskPipeline pipeline,
        Func<TaskContext, Task> execute,
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
    {
        if (execute == null)
            throw new ArgumentNullException(nameof(execute));

        return pipeline.AddTask(
            name ?? "InlineTask",
            (context, _) => execute(context),
            retryCount,
            timeout);
    }

    /// <summary>
    /// Adds a single sequential inline task using a delegate with cancellation support and the default task name.
    /// </summary>
    public static TaskPipeline AddTask(
        this TaskPipeline pipeline,
        Func<TaskContext, CancellationToken, Task> execute,
        int? retryCount = null,
        TimeSpan? timeout = null,
        string? name = null)
    {
        return pipeline.AddTask(
            name ?? "InlineTask",
            execute,
            retryCount,
            timeout);
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

    private sealed class InlineTask : ITask
    {
        private readonly Func<TaskContext, CancellationToken, Task> _execute;

        public InlineTask(Func<TaskContext, CancellationToken, Task> execute)
        {
            _execute = execute ?? throw new ArgumentNullException(nameof(execute));
        }

        public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
        {
            return _execute(context, cancellationToken);
        }
    }
}
