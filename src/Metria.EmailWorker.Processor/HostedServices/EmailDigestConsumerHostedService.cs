using Metria.EmailWorker.Infrastructure.Messaging;

namespace Metria.EmailWorker.Processor.HostedServices;

public sealed class EmailDigestConsumerHostedService : BackgroundService
{
    private readonly RabbitMqConsumer _consumer;
    private readonly EmailDigestMessageProcessor _processor;

    public EmailDigestConsumerHostedService(
        RabbitMqConsumer consumer,
        EmailDigestMessageProcessor processor)
    {
        _consumer = consumer;
        _processor = processor;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _consumer.StartConsumingAsync(_processor.ProcessAsync, stoppingToken);

        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // graceful shutdown
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await _consumer.StopAsync();
        await base.StopAsync(cancellationToken);
    }
}
