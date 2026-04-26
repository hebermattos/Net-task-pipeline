using System.Collections.Generic;
using NetTaskPipeline;
using Xunit;

namespace NetTaskPipeline.Tests;

public sealed class TaskContextTests
{
    [Fact]
    public void Set_WithValidKey_StoresValue()
    {
        var context = new TaskContext();

        context.Set("CustomerId", 123);

        Assert.Equal(123, context.Get<int>("CustomerId"));
    }

    [Fact]
    public void Set_WithExistingKey_ReplacesValue()
    {
        var context = new TaskContext();

        context.Set("Status", "pending");
        context.Set("Status", "approved");

        Assert.Equal("approved", context.Get<string>("Status"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Set_WithInvalidKey_ThrowsArgumentException(string? key)
    {
        var context = new TaskContext();

        Assert.Throws<ArgumentException>(() => context.Set(key!, 123));
    }

    [Fact]
    public void Get_WithMissingKey_ThrowsKeyNotFoundException()
    {
        var context = new TaskContext();

        var exception = Assert.Throws<KeyNotFoundException>(() => context.Get<int>("Missing"));

        Assert.Contains("Missing", exception.Message);
    }

    [Fact]
    public void Get_WithDifferentType_ThrowsInvalidCastException()
    {
        var context = new TaskContext();
        context.Set("CustomerId", 123);

        var exception = Assert.Throws<InvalidCastException>(() => context.Get<string>("CustomerId"));

        Assert.Contains("CustomerId", exception.Message);
        Assert.Contains(nameof(String), exception.Message);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Get_WithInvalidKey_ThrowsArgumentException(string? key)
    {
        var context = new TaskContext();

        Assert.Throws<ArgumentException>(() => context.Get<int>(key!));
    }

    [Fact]
    public void TryGet_WithExistingKeyAndMatchingType_ReturnsTrue()
    {
        var context = new TaskContext();
        context.Set("CustomerName", "John Smith");

        var found = context.TryGet<string>("CustomerName", out var value);

        Assert.True(found);
        Assert.Equal("John Smith", value);
    }

    [Fact]
    public void TryGet_WithMissingKey_ReturnsFalseAndDefaultValue()
    {
        var context = new TaskContext();

        var found = context.TryGet<int>("Missing", out var value);

        Assert.False(found);
        Assert.Equal(default, value);
    }

    [Fact]
    public void TryGet_WithDifferentType_ReturnsFalseAndDefaultValue()
    {
        var context = new TaskContext();
        context.Set("CustomerId", 123);

        var found = context.TryGet<string>("CustomerId", out var value);

        Assert.False(found);
        Assert.Null(value);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void TryGet_WithInvalidKey_ThrowsArgumentException(string? key)
    {
        var context = new TaskContext();

        Assert.Throws<ArgumentException>(() => context.TryGet<int>(key!, out _));
    }
}
