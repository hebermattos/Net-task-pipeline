using NetTaskPipeline;

Console.WriteLine("Simple NetTaskPipeline example");
Console.WriteLine();

var result = await new TaskPipeline()
    .AddTask(new PrintMessageTask("First task"))
    .AddTask(
        new PrintMessageTask("Parallel task A"),
        new PrintMessageTask("Parallel task B"))
    .AddTask(new PrintMessageTask("Last task"))
    .ExecuteAsync();

Console.WriteLine();
Console.WriteLine($"Pipeline success: {result.Success}");
Console.WriteLine($"Total duration: {result.Duration.TotalMilliseconds:N0} ms");

foreach (var taskResult in result.TaskResults)
{
    Console.WriteLine($"- {taskResult.TaskName}: {taskResult.Status} ({taskResult.Duration.TotalMilliseconds:N0} ms)");
}

internal sealed class PrintMessageTask : ITask
{
    private readonly string _message;

    public PrintMessageTask(string message)
    {
        _message = message;
    }

    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        await Task.Delay(500, cancellationToken);
        Console.WriteLine(_message);
    }
}
