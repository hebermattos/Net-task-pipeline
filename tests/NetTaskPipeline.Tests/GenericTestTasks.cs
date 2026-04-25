using NetTaskPipeline;

namespace NetTaskPipeline.Tests;

public sealed class SetGenericTaskValueTask : ITask
{
    public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        context.Set("GenericTaskExecuted", true);
        return Task.CompletedTask;
    }
}

public sealed class SetParallelTaskAValueTask : ITask
{
    public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        context.Set("ParallelTaskAExecuted", true);
        return Task.CompletedTask;
    }
}

public sealed class SetParallelTaskBValueTask : ITask
{
    public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        context.Set("ParallelTaskBExecuted", true);
        return Task.CompletedTask;
    }
}

public sealed class SetFallbackTaskValueTask : ITask
{
    public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        context.Set("FallbackTaskExecuted", true);
        return Task.CompletedTask;
    }
}
