using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace NetTaskPipeline;

/// <summary>
/// Stores values shared between tasks during a pipeline execution.
/// </summary>
public sealed class TaskContext
{
    private readonly ConcurrentDictionary<string, object?> _data = new ConcurrentDictionary<string, object?>();

    /// <summary>
    /// Stores or replaces a value in the context.
    /// </summary>
    public void Set<T>(string key, T value)
    {
        if (string.IsNullOrWhiteSpace(key))
            throw new ArgumentException("The context key cannot be null, empty, or whitespace.", nameof(key));

        _data[key] = value;
    }

    /// <summary>
    /// Gets a value from the context.
    /// </summary>
    public T Get<T>(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
            throw new ArgumentException("The context key cannot be null, empty, or whitespace.", nameof(key));

        if (!_data.TryGetValue(key, out var value))
            throw new KeyNotFoundException($"The key '{key}' was not found in the pipeline context.");

        if (value is T typedValue)
            return typedValue;

        throw new InvalidCastException($"The key '{key}' does not contain a value of type {typeof(T).Name}.");
    }

    /// <summary>
    /// Tries to get a value from the context.
    /// </summary>
    public bool TryGet<T>(string key, out T? value)
    {
        if (string.IsNullOrWhiteSpace(key))
            throw new ArgumentException("The context key cannot be null, empty, or whitespace.", nameof(key));

        if (_data.TryGetValue(key, out var rawValue) && rawValue is T typedValue)
        {
            value = typedValue;
            return true;
        }

        value = default;
        return false;
    }
}
