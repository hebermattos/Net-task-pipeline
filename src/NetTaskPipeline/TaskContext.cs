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
        ValidateKey(key);

        _data[key] = value;
    }

    /// <summary>
    /// Gets a value from the context.
    /// </summary>
    public T Get<T>(string key)
    {
        ValidateKey(key);

        if (!_data.TryGetValue(key, out var value))
            throw new KeyNotFoundException($"The key '{key}' was not found in the pipeline context.");

        if (value is T typedValue)
            return typedValue;

        throw new InvalidCastException($"The key '{key}' does not contain a value of type {typeof(T).Name}.");
    }

    /// <summary>
    /// Removes a value from the context.
    /// </summary>
    public bool Remove(string key)
    {
        ValidateKey(key);
        return _data.TryRemove(key, out _);
    }

    /// <summary>
    /// Checks whether a key exists in the context.
    /// </summary>
    public bool ContainsKey(string key)
    {
        ValidateKey(key);
        return _data.ContainsKey(key);
    }

    /// <summary>
    /// Gets an existing value or atomically adds a new value. The factory may run more than once under contention; avoid side effects.
    /// </summary>
    public T GetOrAdd<T>(string key, Func<string, T> valueFactory)
    {
        ValidateKey(key);
        if (valueFactory == null)
            throw new ArgumentNullException(nameof(valueFactory));

        var value = _data.GetOrAdd(key, k => valueFactory(k));
        if (value is T typedValue)
            return typedValue;

        throw new InvalidCastException($"The key '{key}' does not contain a value of type {typeof(T).Name}.");
    }

    /// <summary>
    /// Tries to get a value from the context.
    /// </summary>
    public bool TryGet<T>(string key, out T? value)
    {
        ValidateKey(key);

        if (_data.TryGetValue(key, out var rawValue) && rawValue is T typedValue)
        {
            value = typedValue;
            return true;
        }

        value = default;
        return false;
    }

    private static void ValidateKey(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
            throw new ArgumentException("The context key cannot be null, empty, or whitespace.", nameof(key));
    }
}
