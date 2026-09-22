using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace NetTaskPipeline;

internal sealed class TaskExecutionOptions
{
    public int RetryCount { get; init; }
    public TimeSpan? Timeout { get; init; }
    public Func<int, TimeSpan>? RetryDelay { get; init; }
    public Func<Exception, bool>? ShouldRetry { get; init; }
}

internal static class TaskExecutionEngine
{
    internal static async Task<TaskExecutionResult> ExecuteAsync(
        ITask task,
        string taskName,
        int groupIndex,
        TaskExecutionOptions options,
        TaskContext context,
        CancellationToken rootCancellationToken,
        CancellationToken executionCancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var startedAt = DateTimeOffset.UtcNow;
        var maxAttempts = options.RetryCount + 1;
        Exception? lastException = null;
        var status = TaskExecutionStatus.Failed;
        var attempts = 0;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            attempts = attempt;
            rootCancellationToken.ThrowIfCancellationRequested();

            if (attempt > 1 && options.RetryDelay != null)
            {
                var delay = options.RetryDelay(attempt - 1);
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, rootCancellationToken).ConfigureAwait(false);
            }

            using var timeoutCancellationTokenSource = CreateTimeoutCancellationTokenSource(executionCancellationToken, options.Timeout);
            try
            {
                await task.ExecuteAsync(context, timeoutCancellationTokenSource.Token).ConfigureAwait(false);
                status = TaskExecutionStatus.Success;
                lastException = null;
                break;
            }
            catch (OperationCanceledException) when (rootCancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException ex)
            {
                if (executionCancellationToken.IsCancellationRequested)
                {
                    lastException = ex;
                    status = TaskExecutionStatus.Canceled;
                    break;
                }

                if (timeoutCancellationTokenSource.IsCancellationRequested)
                {
                    lastException = new TimeoutException($"The task '{taskName}' exceeded the configured timeout of {options.Timeout}.", ex);
                    status = TaskExecutionStatus.Failed;
                }
                else
                {
                    lastException = ex;
                    status = TaskExecutionStatus.Canceled;
                    break;
                }
            }
            catch (Exception ex)
            {
                lastException = ex;
                status = TaskExecutionStatus.Failed;
            }

            if (attempt < maxAttempts && lastException != null && options.ShouldRetry != null && !options.ShouldRetry(lastException))
                break;
        }

        stopwatch.Stop();
        return new TaskExecutionResult
        {
            TaskName = taskName,
            GroupIndex = groupIndex,
            Attempts = attempts,
            Status = status,
            Exception = lastException,
            Duration = stopwatch.Elapsed,
            StartedAt = startedAt,
            FinishedAt = DateTimeOffset.UtcNow
        };
    }

    private static CancellationTokenSource CreateTimeoutCancellationTokenSource(CancellationToken cancellationToken, TimeSpan? timeout)
    {
        var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (timeout.HasValue)
            cancellationTokenSource.CancelAfter(timeout.Value);

        return cancellationTokenSource;
    }
}
