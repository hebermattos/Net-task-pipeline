# Dependency Injection example

This example shows how to execute `ITask` implementations that receive dependencies through constructors.

## Run

From the repository root:

```bash
dotnet run --project examples/DependencyInjectionExample/DependencyInjectionExample.csproj
```

## Usage

Register the dependencies and the tasks in a .NET service collection:

```csharp
var services = new ServiceCollection();

services.AddSingleton<ICustomerRepository, InMemoryCustomerRepository>();
services.AddTransient<LoadCustomerTask>();
services.AddTransient<SendCustomerNotificationTask>();

await using var serviceProvider = services.BuildServiceProvider();
```

Pass the service provider when adding typed tasks:

```csharp
var result = await new TaskPipeline()
    .AddTask<LoadCustomerTask>(serviceProvider)
    .AddTask<SendCustomerNotificationTask>(serviceProvider)
    .ExecuteAsync(context);
```

`LoadCustomerTask` receives `ICustomerRepository` through constructor injection:

```csharp
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
```

The same pattern is also available for `AddParallel<TTask1, TTask2>(serviceProvider)`.
