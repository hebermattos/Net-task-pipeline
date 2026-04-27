# Passing `IServiceProvider` through `TaskPipeline`

`TaskPipeline` can receive an `IServiceProvider` in its constructor. This lets typed task registration methods resolve task instances from dependency injection without passing the provider to every `AddTask` or `AddParallel` call.

## Recommended usage

```csharp
var services = new ServiceCollection();

services.AddSingleton<ICustomerRepository, CustomerRepository>();
services.AddTransient<LoadCustomerTask>();
services.AddTransient<SendCustomerNotificationTask>();

using var serviceProvider = services.BuildServiceProvider();

var result = await new TaskPipeline(serviceProvider)
    .AddTask<LoadCustomerTask>()
    .AddTask<SendCustomerNotificationTask>()
    .ExecuteAsync();
```

## Parallel tasks

The same service provider is used when resolving parallel task groups.

```csharp
var result = await new TaskPipeline(serviceProvider)
    .AddParallel<LoadCustomerTask, SendCustomerNotificationTask>()
    .ExecuteAsync();
```

## Branches

Child pipelines created by `AddBranch` inherit the parent pipeline service provider.

```csharp
var result = await new TaskPipeline(serviceProvider)
    .AddBranch(
        ctx => ctx.Get<bool>("ShouldLoadCustomer"),
        whenTrue: branch => branch.AddTask<LoadCustomerTask>())
    .ExecuteAsync(context);
```

## Backward compatibility

The explicit provider overloads are still available:

```csharp
await new TaskPipeline()
    .AddTask<LoadCustomerTask>(serviceProvider)
    .ExecuteAsync();
```

For tasks without dependency injection, `AddTask<TTask>()` still works when `TTask` has a public parameterless constructor.
