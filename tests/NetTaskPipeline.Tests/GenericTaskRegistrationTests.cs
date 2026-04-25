using NetTaskPipeline;
using Xunit;

namespace NetTaskPipeline.Tests;

public sealed class GenericTaskRegistrationTests
{
    [Fact]
    public async Task ExecuteAsync_WithGenericAddTask_ExecutesTask()
    {
        var result = await new TaskPipeline()
            .AddTask<SetGenericTaskValueTask>()
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.True(result.Context.Get<bool>("GenericTaskExecuted"));
    }

    [Fact]
    public async Task ExecuteAsync_WithGenericAddParallel_ExecutesTasksInParallelGroup()
    {
        var result = await new TaskPipeline()
            .AddParallel<SetParallelTaskAValueTask, SetParallelTaskBValueTask>()
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.True(result.Context.Get<bool>("ParallelTaskAExecuted"));
        Assert.True(result.Context.Get<bool>("ParallelTaskBExecuted"));
        Assert.Equal(2, result.TaskResults.Count);
    }
}
