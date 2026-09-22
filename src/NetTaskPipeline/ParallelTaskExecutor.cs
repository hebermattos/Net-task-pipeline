using System;
using System.Collections.Generic;
using System.Linq;
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
        using var semaphore = new SemaphoreSlim(maxDegreeOfParallelism);

        var executions = items.Select(async item =>
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await execute(item, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                semaphore.Release();
            }
        });

        return await Task.WhenAll(executions).ConfigureAwait(false);
    }
}
