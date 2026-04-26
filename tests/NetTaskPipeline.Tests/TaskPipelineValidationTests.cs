using NetTaskPipeline;
using Xunit;

namespace NetTaskPipeline.Tests;

public sealed class TaskPipelineValidationTests
{
    [Fact]
    public void WithRetry_Negative_Throws()
    {
        var pipeline = new TaskPipeline();

        Assert.Throws<ArgumentOutOfRangeException>(() => pipeline.WithRetry(-1));
    }

    [Fact]
    public void WithTimeout_Invalid_Throws()
    {
        var pipeline = new TaskPipeline();

        Assert.Throws<ArgumentOutOfRangeException>(() => pipeline.WithTimeout(TimeSpan.Zero));
    }

    [Fact]
    public void WithMaxDegreeOfParallelism_Invalid_Throws()
    {
        var pipeline = new TaskPipeline();

        Assert.Throws<ArgumentOutOfRangeException>(() => pipeline.WithMaxDegreeOfParallelism(0));
    }

    [Fact]
    public async Task ExecuteAsync_NullContext_Throws()
    {
        var pipeline = new TaskPipeline();

        await Assert.ThrowsAsync<ArgumentNullException>(() => pipeline.ExecuteAsync(null!));
    }
}
