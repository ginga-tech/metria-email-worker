using System.Diagnostics;
using Microsoft.Extensions.Options;
using Metria.EmailWorker.Application.Abstractions;
using Metria.EmailWorker.Application.Exceptions;
using Metria.EmailWorker.Application.Models;
using Metria.EmailWorker.Application.UseCases;
using Metria.EmailWorker.Infrastructure.Configuration;
using Metria.EmailWorker.Infrastructure.Messaging;
using Metria.EmailWorker.Infrastructure.Observability;
using Polly;

namespace Metria.EmailWorker.Processor.HostedServices;

public sealed class EmailDigestMessageProcessor
{
    private readonly RabbitMqMessageDeserializer _deserializer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly RetryOptions _retryOptions;
    private readonly IEmailWorkerMetrics _metrics;
    private readonly ILogger<EmailDigestMessageProcessor> _logger;

    public EmailDigestMessageProcessor(
        RabbitMqMessageDeserializer deserializer,
        IServiceScopeFactory scopeFactory,
        IOptions<RetryOptions> retryOptions,
        IEmailWorkerMetrics metrics,
        ILogger<EmailDigestMessageProcessor> logger)
    {
        _deserializer = deserializer;
        _scopeFactory = scopeFactory;
        _retryOptions = retryOptions.Value;
        _metrics = metrics;
        _logger = logger;
    }

    public async Task<RabbitMqMessageDisposition> ProcessAsync(
        RabbitMqDeliveryContext deliveryContext,
        CancellationToken cancellationToken)
    {
        IDisposable? logScope = null;
        var retryCount = 0;
        var stopwatch = Stopwatch.StartNew();

        try
        {
            var message = _deserializer.Deserialize(deliveryContext.Body);
            logScope = LoggingScopes.BeginMessageScope(
                _logger,
                message.MessageId,
                message.CorrelationId,
                message.UserId);

            var policy = Policy<ProcessEmailDigestResult>
                .Handle<TransientProcessingException>()
                .WaitAndRetryAsync(
                    Math.Max(_retryOptions.MaxAttempts - 1, 0),
                    attempt => CalculateBackoff(attempt),
                    (outcome, delay, attempt, _) =>
                    {
                        retryCount = attempt;
                        _metrics.IncrementRetried();

                        _logger.LogWarning(
                            outcome.Exception,
                            "Transient failure while processing email digest. retryCount={retryCount} delayMs={delayMs}",
                            retryCount,
                            delay.TotalMilliseconds);
                    });

            var result = await policy.ExecuteAsync(
                async ct =>
                {
                    using var scope = _scopeFactory.CreateScope();
                    var useCase = scope.ServiceProvider.GetRequiredService<ProcessEmailDigestUseCase>();
                    return await useCase.ExecuteAsync(message, ct);
                },
                cancellationToken);

            if (result.Sent)
            {
                _metrics.IncrementSent();
            }

            _logger.LogInformation(
                "Message processed successfully. sent={sent} skippedDuplicate={skippedDuplicate} durationMs={durationMs} retryCount={retryCount}",
                result.Sent,
                result.SkippedDuplicate,
                stopwatch.ElapsedMilliseconds,
                retryCount);

            return RabbitMqMessageDisposition.Ack;
        }
        catch (PermanentProcessingException ex)
        {
            using (logScope ?? LoggingScopes.BeginMessageScope(_logger, Guid.Empty, Guid.Empty, Guid.Empty))
            {
                _logger.LogError(
                    ex,
                    "Permanent failure. Routing message to DLQ. durationMs={durationMs} retryCount={retryCount}",
                    stopwatch.ElapsedMilliseconds,
                    retryCount);
            }

            _metrics.IncrementFailed();
            return RabbitMqMessageDisposition.NackToDlq;
        }
        catch (TransientProcessingException ex)
        {
            using (logScope ?? LoggingScopes.BeginMessageScope(_logger, Guid.Empty, Guid.Empty, Guid.Empty))
            {
                _logger.LogError(
                    ex,
                    "Transient retries exhausted. Routing message to DLQ. durationMs={durationMs} retryCount={retryCount}",
                    stopwatch.ElapsedMilliseconds,
                    retryCount);
            }

            _metrics.IncrementFailed();
            return RabbitMqMessageDisposition.NackToDlq;
        }
        catch (Exception ex)
        {
            using (logScope ?? LoggingScopes.BeginMessageScope(_logger, Guid.Empty, Guid.Empty, Guid.Empty))
            {
                _logger.LogError(
                    ex,
                    "Unexpected failure. Routing message to DLQ. durationMs={durationMs} retryCount={retryCount}",
                    stopwatch.ElapsedMilliseconds,
                    retryCount);
            }

            _metrics.IncrementFailed();
            return RabbitMqMessageDisposition.NackToDlq;
        }
        finally
        {
            logScope?.Dispose();
            _metrics.IncrementProcessed();
            stopwatch.Stop();
        }
    }

    private TimeSpan CalculateBackoff(int attempt)
    {
        var exponent = Math.Pow(2, attempt);
        var delaySeconds = _retryOptions.BaseDelaySeconds * exponent;
        var cappedSeconds = Math.Min(delaySeconds, _retryOptions.MaxDelaySeconds);
        return TimeSpan.FromSeconds(cappedSeconds);
    }
}
