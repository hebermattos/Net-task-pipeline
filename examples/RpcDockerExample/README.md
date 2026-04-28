# RabbitMQ RPC Docker example

This example shows NetTaskPipeline executing typed RPC calls through RabbitMQ.

It runs three containers:

- `rabbitmq`: RabbitMQ broker with management UI
- `consumer`: RabbitMQ RPC worker that listens to `CustomerRequest` and `OrderRequest`
- `main-app`: pipeline app that sends two typed RabbitMQ RPC calls using `AddTaskRpc<TRequest, TResponse>(requestName)`

## Run

From this folder:

```bash
docker compose up --build
```

RabbitMQ Management UI:

```text
http://localhost:15672
user: guest
password: guest
```

## Flow

The main app creates two requests in the shared `TaskContext`:

```csharp
context.Set("CustomerRequest", new GetCustomerRequest { CustomerId = 123 });
context.Set("OrderRequest", new GetOrderRequest { OrderId = 987, CustomerId = 123 });
```

Then it executes two sequential typed RabbitMQ RPC tasks:

```csharp
var result = await new TaskPipeline()
    .WithTimeout(TimeSpan.FromSeconds(20))
    .AddTaskRpc<GetCustomerRequest, GetCustomerResponse>("CustomerRequest")
    .AddTaskRpc<GetOrderRequest, GetOrderResponse>("OrderRequest")
    .ExecuteAsync(context);
```

`AddTaskRpc<TRequest, TResponse>(requestName)` uses RabbitMQ internally. The request name is used as the RabbitMQ queue name and the typed response is stored as `{requestName}Response`:

```csharp
var customerResponse = result.Context.Get<GetCustomerResponse>("CustomerRequestResponse");
var orderResponse = result.Context.Get<GetOrderResponse>("OrderRequestResponse");
```

The consumer listens to both RabbitMQ queues and publishes each response back to the reply queue provided by the caller while preserving the correlation id.
