using NetTaskPipeline;

Console.WriteLine("Simple NetTaskPipeline example");
Console.WriteLine();

var result = await new TaskPipeline()
    .AddTask<FirstTask>()
    .AddParallel<ParallelTaskA, ParallelTaskB>()
    .AddTask<LastTask>()
    .ExecuteAsync();

Console.WriteLine();
Console.WriteLine($"Pipeline success: {result.Success}");
Console.WriteLine($"Total duration: {result.Duration.TotalMilliseconds:N0} ms");

foreach (var taskResult in result.TaskResults)
{
    Console.WriteLine($"- {taskResult.TaskName}: {taskResult.Status} ({taskResult.Duration.TotalMilliseconds:N0} ms)");
}

internal sealed class FirstTask : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        await Task.Delay(500, cancellationToken);
        Console.WriteLine("First task");
    }
}

internal sealed class ParallelTaskA : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        await Task.Delay(500, cancellationToken);
        Console.WriteLine("Parallel task A");
    }
}

internal sealed class ParallelTaskB : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        await Task.Delay(500, cancellationToken);
        Console.WriteLine("Parallel task B");
    }
}

internal sealed class LastTask : ITask
{
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        await Task.Delay(500, cancellationToken);
        Console.WriteLine("Last task");
    }
}
