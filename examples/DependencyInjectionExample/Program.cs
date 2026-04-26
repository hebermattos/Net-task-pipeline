using Microsoft.Extensions.DependencyInjection;
using NetTaskPipeline;

var services = new ServiceCollection();

services.AddSingleton<ICustomerRepository, InMemoryCustomerRepository>();
services.AddTransient<LoadCustomerTask>();
services.AddTransient<SendCustomerNotificationTask>();

await using var serviceProvider = services.BuildServiceProvider();

var context = new TaskContext();
context.Set("CustomerId", 123);

var result = await new TaskPipeline()
    .AddTask<LoadCustomerTask>(serviceProvider)
    .AddTask<SendCustomerNotificationTask>(serviceProvider)
    .ExecuteAsync(context);

Console.WriteLine($"Pipeline success: {result.Success}");
Console.WriteLine($"Customer name: {result.Context.Get<string>("CustomerName")}");
Console.WriteLine($"Notification sent: {result.Context.Get<bool>("NotificationSent")}");

public interface ICustomerRepository
{
    string GetCustomerName(int customerId);
}

public sealed class InMemoryCustomerRepository : ICustomerRepository
{
    public string GetCustomerName(int customerId)
    {
        return $"Customer {customerId}";
    }
}

public sealed class LoadCustomerTask : ITask
{
    private readonly ICustomerRepository _customerRepository;

    public LoadCustomerTask(ICustomerRepository customerRepository)
    {
        _customerRepository = customerRepository;
    }

    public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        var customerId = context.Get<int>("CustomerId");
        var customerName = _customerRepository.GetCustomerName(customerId);

        context.Set("CustomerName", customerName);

        return Task.CompletedTask;
    }
}

public sealed class SendCustomerNotificationTask : ITask
{
    public Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        var customerName = context.Get<string>("CustomerName");

        Console.WriteLine($"Sending notification to {customerName}.");
        context.Set("NotificationSent", true);

        return Task.CompletedTask;
    }
}
