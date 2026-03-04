using Metria.EmailWorker.Application.Abstractions;
using Metria.EmailWorker.Application.Contracts.Messages.V1;
using Metria.EmailWorker.Application.Exceptions;
using Metria.EmailWorker.Application.Models;
using Metria.EmailWorker.Application.Validation;
using Metria.EmailWorker.Domain.Enums;
using Metria.EmailWorker.Domain.Exceptions;

namespace Metria.EmailWorker.Application.UseCases;

public sealed class ProcessEmailDigestUseCase
{
    private readonly EmailDigestMessageValidator _validator;
    private readonly IEmailDispatchRepository _dispatchRepository;
    private readonly IEmailTemplateRenderer _templateRenderer;
    private readonly IEmailSender _emailSender;
    private readonly IClock _clock;

    public ProcessEmailDigestUseCase(
        EmailDigestMessageValidator validator,
        IEmailDispatchRepository dispatchRepository,
        IEmailTemplateRenderer templateRenderer,
        IEmailSender emailSender,
        IClock clock)
    {
        _validator = validator;
        _dispatchRepository = dispatchRepository;
        _templateRenderer = templateRenderer;
        _emailSender = emailSender;
        _clock = clock;
    }

    public async Task<ProcessEmailDigestResult> ExecuteAsync(EmailDigestMessageV1 message, CancellationToken cancellationToken)
    {
        try
        {
            _validator.Validate(message);
        }
        catch (DomainValidationException ex)
        {
            throw new PermanentProcessingException("Message contract validation failed.", ex);
        }

        var reservation = await _dispatchRepository.TryReserveAsync(message, cancellationToken);
        if (reservation is null)
        {
            return ProcessEmailDigestResult.Duplicate(message.MessageId, message.CorrelationId, message.UserId);
        }

        try
        {
            var payload = _templateRenderer.Render(message);
            await _emailSender.SendDigestAsync(payload, cancellationToken);

            await _dispatchRepository.MarkSentAsync(
                reservation.DispatchLogId,
                _clock.UtcNow,
                cancellationToken);

            return ProcessEmailDigestResult.SentOk(message.MessageId, message.CorrelationId, message.UserId);
        }
        catch (PermanentProcessingException)
        {
            await _dispatchRepository.MarkFailedAsync(
                reservation.DispatchLogId,
                EmailDispatchStatus.PermanentFailed.ToString(),
                cancellationToken);
            throw;
        }
        catch (TransientProcessingException)
        {
            await _dispatchRepository.MarkFailedAsync(
                reservation.DispatchLogId,
                EmailDispatchStatus.TransientFailed.ToString(),
                cancellationToken);
            throw;
        }
        catch (Exception ex)
        {
            await _dispatchRepository.MarkFailedAsync(
                reservation.DispatchLogId,
                EmailDispatchStatus.TransientFailed.ToString(),
                cancellationToken);
            throw new TransientProcessingException("Unexpected transient processing failure.", ex);
        }
    }
}
