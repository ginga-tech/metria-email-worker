using System.Text.Json.Serialization;

namespace Metria.EmailWorker.Application.Contracts.Messages.V1;

public sealed class EmailDigestMessageV1
{
    [JsonPropertyName("messageId")]
    public Guid MessageId { get; init; }

    [JsonPropertyName("correlationId")]
    public Guid CorrelationId { get; init; }

    [JsonPropertyName("userId")]
    public Guid UserId { get; init; }

    [JsonPropertyName("email")]
    public string Email { get; init; } = string.Empty;

    [JsonPropertyName("firstName")]
    public string? FirstName { get; init; }

    [JsonPropertyName("locale")]
    public string Locale { get; init; } = string.Empty;

    [JsonPropertyName("timeZone")]
    public string TimeZone { get; init; } = string.Empty;

    [JsonPropertyName("periodStartUtc")]
    public DateTime PeriodStartUtc { get; init; }

    [JsonPropertyName("periodEndUtc")]
    public DateTime PeriodEndUtc { get; init; }

    [JsonPropertyName("templateKey")]
    public string TemplateKey { get; init; } = string.Empty;

    [JsonPropertyName("metadata")]
    public Dictionary<string, string> MetadataMap { get; init; } = new(StringComparer.OrdinalIgnoreCase);

    [JsonIgnore]
    public IReadOnlyDictionary<string, string> Metadata => MetadataMap;
}
