using System;
using System.Threading;

namespace NetTaskPipeline;

internal static class RetryDelayStrategy
{
    private static readonly ThreadLocal<Random> RandomSource =
        new ThreadLocal<Random>(() => new Random(unchecked(Environment.TickCount * 31 + Thread.CurrentThread.ManagedThreadId)));

    internal static Func<int, TimeSpan> Create(TimeSpan delay, bool exponentialBackoff, bool jitter)
    {
        return attempt =>
        {
            var multiplier = exponentialBackoff ? Math.Pow(2, Math.Min(attempt - 1, 20)) : 1d;
            var ticks = Math.Min(delay.Ticks * multiplier, TimeSpan.MaxValue.Ticks);

            if (jitter && ticks > 0)
                ticks *= 0.5d + RandomSource.Value!.NextDouble() * 0.5d;

            return TimeSpan.FromTicks((long)ticks);
        };
    }
}
