using NetTaskPipeline;
using Xunit;

namespace NetTaskPipeline.Tests;

public sealed class TaskPipelineResultTests
{
    [Fact]
    public void Success_WhenAllTasksSuccessful_ReturnsTrue()
    {
        var results = new[]
        {
            new TaskExecutionResult { Status = TaskExecutionStatus.Success },
            new TaskExecutionResult { Status = TaskExecutionStatus.Success }
        };

        var result = new TaskPipelineResult(results, new TaskContext(), TimeSpan.Zero);

        Assert.True(result.Success);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Success_WhenAnyFails_ReturnsFalse()
    {
        var results = new[]
        {
            new TaskExecutionResult { Status = TaskExecutionStatus.Success },
            new TaskExecutionResult { Status = TaskExecutionStatus.Failed }
        };

        var result = new TaskPipelineResult(results, new TaskContext(), TimeSpan.Zero);

        Assert.False(result.Success);
        Assert.Single(result.Errors);
    }

    [Fact]
    public void Constructor_NullArgs_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new TaskPipelineResult(null!, new TaskContext(), TimeSpan.Zero));

        Assert.Throws<ArgumentNullException>(() =>
            new TaskPipelineResult(new List<TaskExecutionResult>(), null!, TimeSpan.Zero));
    }
}
