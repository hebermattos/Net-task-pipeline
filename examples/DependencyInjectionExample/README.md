# Dependency Injection example

This example shows how to execute `ITask` implementations that receive dependencies through constructors.

The complete dependency injection guide is available at:

```text
docs/dependency-injection.md
```

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

## How task resolution works

When the pipeline receives `AddTask<TTask>(serviceProvider)`, it resolves the task using this order:

1. Try to get `TTask` directly from the service provider.
2. If `TTask` is not registered, inspect public constructors.
3. Use a constructor where all parameters can be resolved from the service provider.
4. Throw an exception if the task cannot be created.

So this works when the task is explicitly registered:

```csharp
services.AddTransient<LoadCustomerTask>();
```

This can also work when only the constructor dependencies are registered:

```csharp
services.AddSingleton<ICustomerRepository, InMemoryCustomerRepository>();
```

The same pattern is also available for `AddParallel<TTask1, TTask2>(serviceProvider)`.

## Scoped services

When tasks depend on scoped services, such as a database context, pass a scoped service provider.

```csharp
using var scope = serviceProvider.CreateScope();

var result = await new TaskPipeline()
    .AddTask<LoadCustomerTask>(scope.ServiceProvider)
    .ExecuteAsync(context);
```
