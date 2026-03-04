using Metria.EmailWorker.Application.Contracts.Messages.V1;
using Metria.EmailWorker.Domain.Exceptions;
using Metria.EmailWorker.Domain.ValueObjects;

namespace Metria.EmailWorker.Application.Validation;

public sealed class EmailDigestMessageValidator
{
    public void Validate(EmailDigestMessageV1 message)
    {
        if (message.MessageId == Guid.Empty)
        {
            throw new DomainValidationException("MessageId must not be empty.");
        }

        if (message.CorrelationId == Guid.Empty)
        {
            throw new DomainValidationException("CorrelationId must not be empty.");
        }

        if (message.UserId == Guid.Empty)
        {
            throw new DomainValidationException("UserId must not be empty.");
        }

        if (string.IsNullOrWhiteSpace(message.Locale))
        {
            throw new DomainValidationException("Locale must be provided.");
        }

        if (string.IsNullOrWhiteSpace(message.TimeZone))
        {
            throw new DomainValidationException("TimeZone must be provided.");
        }

        _ = new EmailAddress(message.Email);
        _ = new Period(EnsureUtc(message.PeriodStartUtc), EnsureUtc(message.PeriodEndUtc));
        _ = new TemplateKey(message.TemplateKey);
    }

    private static DateTime EnsureUtc(DateTime value) =>
        value.Kind switch
        {
            DateTimeKind.Utc => value,
            DateTimeKind.Local => value.ToUniversalTime(),
            _ => DateTime.SpecifyKind(value, DateTimeKind.Utc)
        };
}
