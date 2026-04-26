# Dependency injection with `ITask`

`NetTaskPipeline` can execute `ITask` implementations that receive dependencies through constructor injection.

This is useful when a task needs services from a .NET application, such as repositories, API clients, loggers, configuration objects, or domain services.

The library uses `Microsoft.Extensions.DependencyInjection` and `ActivatorUtilities.GetServiceOrCreateInstance<TTask>()` to resolve tasks.

## Basic idea

Instead of creating the task manually, pass an `IServiceProvider` to the typed task registration overload:

```csharp
var result = await new TaskPipeline()
    .AddTask<LoadCustomerTask>(serviceProvider)
    .ExecuteAsync(context);
```

The pipeline resolves `LoadCustomerTask` using the provided service provider.

## Registering services

In a console app, worker service, or ASP.NET Core app, register the dependencies and the task types in the normal .NET DI container.

```csharp
var services = new ServiceCollection();

services.AddSingleton<ICustomerRepository, CustomerRepository>();
services.AddTransient<LoadCustomerTask>();
services.AddTransient<SendCustomerNotificationTask>();

await using var serviceProvider = services.BuildServiceProvider();
```

Then pass the provider to `AddTask<TTask>(serviceProvider)`:

```csharp
var context = new TaskContext();
context.Set("CustomerId", 123);

var result = await new TaskPipeline()
    .AddTask<LoadCustomerTask>(serviceProvider)
    .AddTask<SendCustomerNotificationTask>(serviceProvider)
    .ExecuteAsync(context);
```

## Task with constructor dependencies

A task can receive dependencies in its constructor:

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

The repository is resolved from the service provider:

```csharp
services.AddSingleton<ICustomerRepository, CustomerRepository>();
services.AddTransient<LoadCustomerTask>();
```

## Task does not have to be explicitly registered

The task type itself can be registered:

```csharp
services.AddTransient<LoadCustomerTask>();
```

But `ActivatorUtilities.GetServiceOrCreateInstance<TTask>()` can also create the task if all constructor parameters are registered.

For example, this can work:

```csharp
services.AddSingleton<ICustomerRepository, CustomerRepository>();
```

Even without registering `LoadCustomerTask`, the pipeline can create it because `ICustomerRepository` is available.

## Parallel tasks with DI

The same pattern is available for two-task parallel groups:

```csharp
var result = await new TaskPipeline()
    .AddParallel<LoadCustomerTask, SendCustomerNotificationTask>(serviceProvider)
    .ExecuteAsync(context);
```

Both task types are resolved through the same service provider.

## ASP.NET Core usage

In ASP.NET Core, use the request service provider when tasks need scoped services, such as `DbContext`.

```csharp
app.MapPost("/customers/{id}/process", async (
    int id,
    IServiceProvider serviceProvider) =>
{
    var context = new TaskContext();
    context.Set("CustomerId", id);

    var result = await new TaskPipeline()
        .AddTask<LoadCustomerTask>(serviceProvider)
        .AddTask<SendCustomerNotificationTask>(serviceProvider)
        .ExecuteAsync(context);

    return Results.Ok(new
    {
        result.Success
    });
});
```

For scoped services, prefer the scoped provider from the request instead of building a root provider manually.

## Worker Service usage

In a worker service, inject `IServiceProvider` and create a scope when tasks use scoped dependencies.

```csharp
using var scope = serviceProvider.CreateScope();
var scopedProvider = scope.ServiceProvider;

var result = await new TaskPipeline()
    .AddTask<LoadCustomerTask>(scopedProvider)
    .ExecuteAsync(cancellationToken);
```

## Resolution behavior

When `AddTask<TTask>(serviceProvider)` is used, task creation is delegated to:

```csharp
ActivatorUtilities.GetServiceOrCreateInstance<TTask>(serviceProvider)
```

This means:

1. If `TTask` is registered in the container, the registered service is used.
2. If `TTask` is not registered, `ActivatorUtilities` tries to instantiate it.
3. Constructor parameters are resolved from the service provider.
4. An exception is thrown if required dependencies cannot be resolved.

## Current overloads

Available DI-aware overloads:

```csharp
AddTask<TTask>(IServiceProvider serviceProvider)
AddParallel<TTask1, TTask2>(IServiceProvider serviceProvider)
```

The existing non-DI generic overloads still work for tasks that have a public parameterless constructor:

```csharp
AddTask<ValidateCustomerTask>()
AddParallel<TaskA, TaskB>()
```

## Complete runnable example

A complete runnable example is available in:

```text
examples/DependencyInjectionExample
```

Run it from the repository root:

```bash
dotnet run --project examples/DependencyInjectionExample/DependencyInjectionExample.csproj
```
