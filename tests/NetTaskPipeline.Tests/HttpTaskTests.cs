using System.Net;
using System.Net.Http;
using System.Text;
using NetTaskPipeline;
using Xunit;

namespace NetTaskPipeline.Tests;

public sealed class HttpTaskTests
{
    [Fact]
    public async Task ExecuteAsync_WithHttpTask_StoresTypedResponse()
    {
        HttpRequestMessage? capturedRequest = null;

        var context = new TaskContext();
        context.Set("CustomerRequest", new GetCustomerRequest { CustomerId = 123 });

        var result = await new TaskPipeline()
            .AddTaskHttp<GetCustomerRequest, GetCustomerResponse>(
                ctx => ctx.Get<GetCustomerRequest>("CustomerRequest"),
                options =>
                {
                    options.RequestUri = "https://api.example.com/customers";
                    options.Method = HttpMethod.Post;
                    options.ResponseKey = "CustomerResponse";
                    options.Headers["x-api-key"] = "secret";
                    options.MessageHandler = new StubHttpMessageHandler(async (request, _) =>
                    {
                        capturedRequest = request;

                        var requestContent = request.Content == null
                            ? string.Empty
                            : await request.Content.ReadAsStringAsync();

                        Assert.Equal(HttpMethod.Post, request.Method);
                        Assert.Equal("https://api.example.com/customers", request.RequestUri!.ToString());
                        Assert.Contains("\"CustomerId\":123", requestContent);
                        Assert.True(request.Headers.Contains("x-api-key"));

                        return new HttpResponseMessage(HttpStatusCode.OK)
                        {
                            Content = new StringContent(
                                "{\"CustomerId\":123,\"Name\":\"John Smith\"}",
                                Encoding.UTF8,
                                "application/json")
                        };
                    });
                },
                name: "Load customer")
            .ExecuteAsync(context);

        var taskResult = Assert.Single(result.TaskResults);
        var response = result.Context.Get<GetCustomerResponse>("CustomerResponse");

        Assert.True(result.Success);
        Assert.NotNull(capturedRequest);
        Assert.Equal("Load customer", taskResult.TaskName);
        Assert.Equal(123, response.CustomerId);
        Assert.Equal("John Smith", response.Name);
    }

    [Fact]
    public async Task ExecuteAsync_WithHttpTaskStringResponse_StoresRawContent()
    {
        var result = await new TaskPipeline()
            .AddTaskHttp<object, string>(
                _ => new object(),
                options =>
                {
                    options.RequestUri = "https://api.example.com/health";
                    options.Method = HttpMethod.Get;
                    options.ResponseKey = "HealthResponse";
                    options.MessageHandler = new StubHttpMessageHandler((_, _) =>
                        Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                        {
                            Content = new StringContent("healthy", Encoding.UTF8, "text/plain")
                        }));
                })
            .ExecuteAsync();

        Assert.True(result.Success);
        Assert.Equal("healthy", result.Context.Get<string>("HealthResponse"));
    }

    [Fact]
    public async Task ExecuteAsync_WithHttpTaskUsingCancellationToken_PassesCancellationToken()
    {
        using var cancellationTokenSource = new CancellationTokenSource();
        var receivedCancellationToken = CancellationToken.None;

        var result = await new TaskPipeline()
            .AddTaskHttp<object, string>(
                (_, cancellationToken) =>
                {
                    receivedCancellationToken = cancellationToken;
                    return Task.FromResult<object>(new object());
                },
                options =>
                {
                    options.RequestUri = "https://api.example.com/health";
                    options.Method = HttpMethod.Get;
                    options.ResponseKey = "HealthResponse";
                    options.MessageHandler = new StubHttpMessageHandler((_, token) =>
                    {
                        receivedCancellationToken = token;
                        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                        {
                            Content = new StringContent("healthy", Encoding.UTF8, "text/plain")
                        });
                    });
                })
            .ExecuteAsync(cancellationTokenSource.Token);

        Assert.True(result.Success);
        Assert.True(receivedCancellationToken.CanBeCanceled);
    }

    [Fact]
    public async Task ExecuteAsync_WithHttpTaskFailure_ReturnsFailedResult()
    {
        var result = await new TaskPipeline()
            .AddTaskHttp<object, string>(
                _ => new object(),
                options =>
                {
                    options.RequestUri = "https://api.example.com/failure";
                    options.Method = HttpMethod.Get;
                    options.MessageHandler = new StubHttpMessageHandler((_, _) =>
                        Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError)
                        {
                            Content = new StringContent("failure", Encoding.UTF8, "text/plain")
                        }));
                })
            .ExecuteAsync();

        var taskResult = Assert.Single(result.TaskResults);

        Assert.False(result.Success);
        Assert.Equal(TaskExecutionStatus.Failed, taskResult.Status);
        Assert.IsType<HttpRequestException>(taskResult.Exception);
    }

    [Fact]
    public async Task ExecuteAsync_WithInvalidHttpOptions_ReturnsFailedResult()
    {
        var result = await new TaskPipeline()
            .AddTaskHttp<object, string>(
                _ => new object(),
                options =>
                {
                    options.RequestUri = "";
                    options.Method = HttpMethod.Get;
                })
            .ExecuteAsync();

        var taskResult = Assert.Single(result.TaskResults);

        Assert.False(result.Success);
        Assert.Equal(TaskExecutionStatus.Failed, taskResult.Status);
        Assert.IsType<ArgumentException>(taskResult.Exception);
    }

    [Fact]
    public void AddTaskHttp_WithNullPipeline_ThrowsArgumentNullException()
    {
        TaskPipeline pipeline = null!;

        Assert.Throws<ArgumentNullException>(() =>
            pipeline.AddTaskHttp<object, string>(_ => new object(), _ => { }));
    }

    [Fact]
    public void AddTaskHttp_WithNullRequestFactory_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new TaskPipeline().AddTaskHttp<object, string>((Func<TaskContext, object>)null!, _ => { }));
    }

    [Fact]
    public void AddTaskHttp_WithNullConfigure_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new TaskPipeline().AddTaskHttp<object, string>(_ => new object(), null!));
    }

    private sealed class StubHttpMessageHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _handler;

        public StubHttpMessageHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> handler)
        {
            _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return _handler(request, cancellationToken);
        }
    }

    private sealed class GetCustomerRequest
    {
        public int CustomerId { get; set; }
    }

    private sealed class GetCustomerResponse
    {
        public int CustomerId { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
