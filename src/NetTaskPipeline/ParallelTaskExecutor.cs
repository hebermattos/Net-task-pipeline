using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NetTaskPipeline;

internal static class ParallelTaskExecutor
{
    internal static async Task<TResult[]> ExecuteAsync<TItem, TResult>(
        IReadOnlyList<TItem> items,
        int maxDegreeOfParallelism,
        Func<TItem, CancellationToken, Task<TResult>> execute,
        CancellationToken cancellationToken)
    {
        var results = new TResult[items.Count];
        var nextIndex = -1;
        var workerCount = Math.Min(maxDegreeOfParallelism, items.Count);
        var workers = new Task[workerCount];

        for (var workerIndex = 0; workerIndex < workerCount; workerIndex++)
            workers[workerIndex] = RunWorkerAsync();

        await Task.WhenAll(workers).ConfigureAwait(false);
        return results;

        async Task RunWorkerAsync()
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var index = Interlocked.Increment(ref nextIndex);
                if (index >= items.Count)
                    return;

                results[index] = await execute(items[index], cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
