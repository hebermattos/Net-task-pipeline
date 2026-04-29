using System.Text.Json.Serialization;
using NetTaskPipeline;

const string name = "michael";

Console.WriteLine("HTTP NetTaskPipeline example");
Console.WriteLine();
Console.WriteLine($"Using public APIs with the name '{name}'.");
Console.WriteLine();

var context = new TaskContext();
context.Set("Name", name);

var result = await new TaskPipeline()
    .AddTask("Prepare input", ctx =>
    {
        var inputName = ctx.Get<string>("Name");
        Console.WriteLine($"Preparing requests for: {inputName}");
        return Task.CompletedTask;
    })
    .AddTaskHttp<object, AgifyResponse>(
        _ => new object(),
        options =>
        {
            options.RequestUri = $"https://api.agify.io/?name={Uri.EscapeDataString(name)}";
            options.Method = HttpMethod.Get;
            options.ResponseKey = "AgePrediction";
        },
        name: "Call Agify")
    .AddTaskHttp<object, NationalizeResponse>(
        _ => new object(),
        options =>
        {
            options.RequestUri = $"https://api.nationalize.io/?name={Uri.EscapeDataString(name)}";
            options.Method = HttpMethod.Get;
            options.ResponseKey = "NationalityPrediction";
        },
        name: "Call Nationalize")
    .AddTask("Print results", ctx =>
    {
        var age = ctx.Get<AgifyResponse>("AgePrediction");
        var nationality = ctx.Get<NationalizeResponse>("NationalityPrediction");
        var topCountry = nationality.Country
            .OrderByDescending(country => country.Probability)
            .FirstOrDefault();

        Console.WriteLine();
        Console.WriteLine($"Predicted age for {age.Name}: {age.Age?.ToString() ?? "unknown"}");

        if (topCountry == null)
        {
            Console.WriteLine($"No nationality prediction returned for {nationality.Name}.");
        }
        else
        {
            Console.WriteLine(
                $"Top nationality prediction for {nationality.Name}: {topCountry.CountryId} ({topCountry.Probability:P2})");
        }

        return Task.CompletedTask;
    })
    .ExecuteAsync(context);

Console.WriteLine();
Console.WriteLine($"Pipeline success: {result.Success}");
Console.WriteLine($"Total duration: {result.Duration.TotalMilliseconds:N0} ms");

foreach (var taskResult in result.TaskResults)
{
    Console.WriteLine($"- {taskResult.TaskName}: {taskResult.Status} ({taskResult.Duration.TotalMilliseconds:N0} ms)");
}

public sealed class AgifyResponse
{
    public int? Age { get; set; }

    public string Name { get; set; } = string.Empty;

    public int Count { get; set; }
}

public sealed class NationalizeResponse
{
    public string Name { get; set; } = string.Empty;

    public List<NationalizeCountryPrediction> Country { get; set; } = new List<NationalizeCountryPrediction>();
}

public sealed class NationalizeCountryPrediction
{
    [JsonPropertyName("country_id")]
    public string CountryId { get; set; } = string.Empty;

    public decimal Probability { get; set; }
}
