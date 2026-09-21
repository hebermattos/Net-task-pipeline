using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace NetTaskPipeline;

internal static class TaskExecutionEngine
{
    internal static async Task<TaskExecutionResult> ExecuteAsync(
        ITask task,
        string taskName,
        int groupIndex,
        int retryCount,
        TimeSpan? timeout,
        Func<int, TimeSpan>? retryDelay,
        TaskContext context,
        CancellationToken rootCancellationToken,
        CancellationToken executionCancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var startedAt = DateTimeOffset.UtcNow;
        var maxAttempts = retryCount + 1;
        Exception? lastException = null;
        var status = TaskExecutionStatus.Failed;
        var attempts = 0;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            attempts = attempt;
            rootCancellationToken.ThrowIfCancellationRequested();

            if (attempt > 1 && retryDelay != null)
            {
                var delay = retryDelay(attempt - 1);
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, rootCancellationToken).ConfigureAwait(false);
            }

            using var timeoutCancellationTokenSource = CreateTimeoutCancellationTokenSource(executionCancellationToken, timeout);
            try
            {
                await task.ExecuteAsync(context, timeoutCancellationTokenSource.Token).ConfigureAwait(false);
                status = TaskExecutionStatus.Success;
                lastException = null;
                break;
            }
            catch (OperationCanceledException ex) when (!rootCancellationToken.IsCancellationRequested)
            {
                if (timeoutCancellationTokenSource.IsCancellationRequested && !executionCancellationToken.IsCancellationRequested)
                {
                    lastException = new TimeoutException($"The task '{taskName}' exceeded the configured timeout of {timeout}.", ex);
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

    private static CancellationTokenSource CreateTimeoutCancellationTokenSource(
        CancellationToken cancellationToken,
        TimeSpan? timeout)
    {
        var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (timeout.HasValue)
            cancellationTokenSource.CancelAfter(timeout.Value);

        return cancellationTokenSource;
    }
}
