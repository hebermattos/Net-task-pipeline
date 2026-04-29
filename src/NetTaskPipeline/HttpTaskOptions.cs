using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;

namespace NetTaskPipeline;

/// <summary>
/// Defines the HTTP options used by <see cref="HttpTask{TRequest,TResponse}"/>.
/// </summary>
public sealed class HttpTaskOptions
{
    /// <summary>
    /// Gets or sets the absolute or relative request URI.
    /// </summary>
    public string RequestUri { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the HTTP method used for the request.
    /// </summary>
    public HttpMethod Method { get; set; } = HttpMethod.Post;

    /// <summary>
    /// Gets the request headers added to the outgoing HTTP request.
    /// </summary>
    public IDictionary<string, string> Headers { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets or sets the request content type used when a request body is sent.
    /// </summary>
    public string ContentType { get; set; } = "application/json";

    /// <summary>
    /// Gets or sets whether <see cref="HttpResponseMessage.EnsureSuccessStatusCode"/> should be called.
    /// </summary>
    public bool EnsureSuccessStatusCode { get; set; } = true;

    /// <summary>
    /// Gets or sets the context key used to store the HTTP response.
    /// </summary>
    public string ResponseKey { get; set; } = "HttpResponse";

    /// <summary>
    /// Gets or sets an optional custom message handler used to create the HTTP client.
    /// </summary>
    public HttpMessageHandler? MessageHandler { get; set; }

    /// <summary>
    /// Gets or sets the JSON serializer options used for request and response payloads.
    /// </summary>
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true
    };
}
