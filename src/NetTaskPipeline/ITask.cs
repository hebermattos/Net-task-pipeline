using System.Threading;
using System.Threading.Tasks;

namespace NetTaskPipeline;

/// <summary>
/// Represents a unit of work that can be executed by a task pipeline.
/// </summary>
public interface ITask
{
    /// <summary>
    /// Executes the task asynchronously.
    /// </summary>
    /// <param name="context">The shared pipeline context.</param>
    /// <param name="cancellationToken">A token used to cancel the task execution.</param>
    Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default);
}
