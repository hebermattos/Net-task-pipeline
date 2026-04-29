using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace NetTaskPipeline;

/// <summary>
/// Executes an HTTP request and stores the response in the shared pipeline context.
/// </summary>
public sealed class HttpTask<TRequest, TResponse> : ITask
{
    private readonly Func<TaskContext, CancellationToken, Task<TRequest>> _requestFactory;
    private readonly HttpTaskOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="HttpTask{TRequest,TResponse}"/> class.
    /// </summary>
    public HttpTask(
        Func<TaskContext, CancellationToken, Task<TRequest>> requestFactory,
        HttpTaskOptions options)
    {
        _requestFactory = requestFactory ?? throw new ArgumentNullException(nameof(requestFactory));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public async Task ExecuteAsync(TaskContext context, CancellationToken cancellationToken = default)
    {
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        ValidateOptions(_options);

        var requestPayload = await _requestFactory(context, cancellationToken).ConfigureAwait(false);

        using var request = new HttpRequestMessage(_options.Method, _options.RequestUri);

        if (ShouldAttachContent(_options.Method))
        {
            var requestJson = JsonSerializer.Serialize(requestPayload, _options.JsonSerializerOptions);
            request.Content = new StringContent(requestJson, Encoding.UTF8, _options.ContentType);
        }

        ApplyHeaders(request, _options);

        using var httpClient = CreateHttpClient(_options);
        using var response = await httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);

        if (_options.EnsureSuccessStatusCode)
            response.EnsureSuccessStatusCode();

        var responseContent = response.Content == null
            ? string.Empty
            : await response.Content.ReadAsStringAsync().ConfigureAwait(false);

        var responsePayload = DeserializeResponse(responseContent, _options.JsonSerializerOptions);
        context.Set(_options.ResponseKey, responsePayload);
    }

    private static HttpClient CreateHttpClient(HttpTaskOptions options)
    {
        return options.MessageHandler == null
            ? new HttpClient()
            : new HttpClient(options.MessageHandler, disposeHandler: false);
    }

    private static void ApplyHeaders(HttpRequestMessage request, HttpTaskOptions options)
    {
        foreach (var header in options.Headers)
        {
            if (request.Headers.TryAddWithoutValidation(header.Key, header.Value))
                continue;

            if (request.Content != null)
                request.Content.Headers.TryAddWithoutValidation(header.Key, header.Value);
        }
    }

    private static bool ShouldAttachContent(HttpMethod method)
    {
        return method == HttpMethod.Post
            || method == HttpMethod.Put
            || string.Equals(method.Method, "PATCH", StringComparison.OrdinalIgnoreCase);
    }

    private static TResponse DeserializeResponse(string responseContent, JsonSerializerOptions serializerOptions)
    {
        if (typeof(TResponse) == typeof(string))
            return (TResponse)(object)responseContent;

        if (string.IsNullOrWhiteSpace(responseContent))
            throw new InvalidOperationException("The HTTP response body is empty.");

        var response = JsonSerializer.Deserialize<TResponse>(responseContent, serializerOptions);
        if (response == null)
            throw new InvalidOperationException("The HTTP response could not be deserialized.");

        return response;
    }

    private static void ValidateOptions(HttpTaskOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.RequestUri))
            throw new ArgumentException("HTTP request URI is required.", nameof(options));

        if (options.Method == null)
            throw new ArgumentException("HTTP method is required.", nameof(options));

        if (string.IsNullOrWhiteSpace(options.ResponseKey))
            throw new ArgumentException("HTTP response key is required.", nameof(options));

        if (ShouldAttachContent(options.Method) && string.IsNullOrWhiteSpace(options.ContentType))
            throw new ArgumentException("HTTP content type is required when a request body is sent.", nameof(options));
    }
}
