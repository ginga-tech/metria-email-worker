using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Metria.EmailWorker.Application.Abstractions;
using Metria.EmailWorker.Application.Exceptions;
using Metria.EmailWorker.Application.Models;
using Metria.EmailWorker.Application.UseCases;
using Metria.EmailWorker.Application.Validation;
using Metria.EmailWorker.Infrastructure.Configuration;
using Metria.EmailWorker.Infrastructure.Messaging;
using Metria.EmailWorker.Infrastructure.Observability;
using Metria.EmailWorker.Processor.HostedServices;

namespace Metria.EmailWorker.Application.UnitTests;

public sealed class EmailDigestMessageProcessorTests
{
    [Fact]
    public async Task ProcessAsync_WhenPermanentFailure_ShouldRouteToDlq()
    {
        // Arrange
        var repository = new Mock<IEmailDispatchRepository>();
        var sender = new Mock<IEmailSender>();
        var renderer = new Mock<IEmailTemplateRenderer>();
        var clock = new Mock<IClock>();
        var metrics = new Mock<IEmailWorkerMetrics>();

        var reservation = new EmailDispatchReservation(Guid.NewGuid(), Guid.NewGuid(), true);
        repository.Setup(x => x.TryReserveAsync(It.IsAny<Metria.EmailWorker.Application.Contracts.Messages.V1.EmailDigestMessageV1>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(reservation);
        renderer.Setup(x => x.Render(It.IsAny<Metria.EmailWorker.Application.Contracts.Messages.V1.EmailDigestMessageV1>()))
            .Throws(new PermanentProcessingException("template not supported"));

        var serviceProvider = BuildServiceProvider(repository, sender, renderer, clock);
        var processor = CreateProcessor(serviceProvider, metrics.Object);
        var deliveryContext = CreateDeliveryContext();

        // Act
        var disposition = await processor.ProcessAsync(deliveryContext, CancellationToken.None);

        // Assert
        disposition.Should().Be(RabbitMqMessageDisposition.NackToDlq);
        metrics.Verify(x => x.IncrementFailed(), Times.Once);
    }

    [Fact]
    public async Task ProcessAsync_WhenTransientFailure_ShouldRetryThenRouteToDlq()
    {
        // Arrange
        var repository = new Mock<IEmailDispatchRepository>();
        var sender = new Mock<IEmailSender>();
        var renderer = new Mock<IEmailTemplateRenderer>();
        var clock = new Mock<IClock>();
        var metrics = new Mock<IEmailWorkerMetrics>();

        var reservation = new EmailDispatchReservation(Guid.NewGuid(), Guid.NewGuid(), true);
        var payload = new EmailSendPayload(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), "user@example.com", "sub", "<p>body</p>", "body");

        repository.Setup(x => x.TryReserveAsync(It.IsAny<Metria.EmailWorker.Application.Contracts.Messages.V1.EmailDigestMessageV1>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(reservation);
        renderer.Setup(x => x.Render(It.IsAny<Metria.EmailWorker.Application.Contracts.Messages.V1.EmailDigestMessageV1>()))
            .Returns(payload);
        sender.Setup(x => x.SendDigestAsync(It.IsAny<EmailSendPayload>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TransientProcessingException("smtp timeout"));

        var serviceProvider = BuildServiceProvider(repository, sender, renderer, clock);
        var processor = CreateProcessor(serviceProvider, metrics.Object, maxAttempts: 3);
        var deliveryContext = CreateDeliveryContext();

        // Act
        var disposition = await processor.ProcessAsync(deliveryContext, CancellationToken.None);

        // Assert
        disposition.Should().Be(RabbitMqMessageDisposition.NackToDlq);
        metrics.Verify(x => x.IncrementRetried(), Times.Exactly(2));
        metrics.Verify(x => x.IncrementFailed(), Times.Once);
    }

    [Fact]
    public void MetricsCounters_ShouldBeCallable()
    {
        // Arrange
        using var metrics = new EmailWorkerMetrics();

        // Act
        var action = () =>
        {
            metrics.IncrementProcessed();
            metrics.IncrementSent();
            metrics.IncrementFailed();
            metrics.IncrementRetried();
        };

        // Assert
        action.Should().NotThrow();
    }

    private static ServiceProvider BuildServiceProvider(
        Mock<IEmailDispatchRepository> repository,
        Mock<IEmailSender> sender,
        Mock<IEmailTemplateRenderer> renderer,
        Mock<IClock> clock)
    {
        var services = new ServiceCollection();
        services.AddSingleton(new EmailDigestMessageValidator());
        services.AddSingleton(repository.Object);
        services.AddSingleton(sender.Object);
        services.AddSingleton(renderer.Object);
        services.AddSingleton(clock.Object);
        services.AddScoped<ProcessEmailDigestUseCase>();

        return services.BuildServiceProvider();
    }

    private static EmailDigestMessageProcessor CreateProcessor(
        ServiceProvider serviceProvider,
        IEmailWorkerMetrics metrics,
        int maxAttempts = 3)
    {
        return new EmailDigestMessageProcessor(
            new RabbitMqMessageDeserializer(),
            serviceProvider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new RetryOptions
            {
                MaxAttempts = maxAttempts,
                BaseDelaySeconds = 1,
                MaxDelaySeconds = 2
            }),
            metrics,
            NullLogger<EmailDigestMessageProcessor>.Instance);
    }

    private static RabbitMqDeliveryContext CreateDeliveryContext()
    {
        var json =
            """
            {
              "messageId": "11111111-1111-1111-1111-111111111111",
              "correlationId": "22222222-2222-2222-2222-222222222222",
              "userId": "33333333-3333-3333-3333-333333333333",
              "email": "user@example.com",
              "firstName": "Test",
              "locale": "en-US",
              "timeZone": "UTC",
              "periodStartUtc": "2026-02-20T00:00:00Z",
              "periodEndUtc": "2026-02-27T00:00:00Z",
              "templateKey": "digest.v1",
              "metadata": {}
            }
            """;

        return new RabbitMqDeliveryContext(1, System.Text.Encoding.UTF8.GetBytes(json), false, "email.digest");
    }
}
