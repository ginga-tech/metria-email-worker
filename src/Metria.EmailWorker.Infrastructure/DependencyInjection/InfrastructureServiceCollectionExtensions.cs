using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Metria.EmailWorker.Application.Abstractions;
using Metria.EmailWorker.Application.UseCases;
using Metria.EmailWorker.Application.Validation;
using Metria.EmailWorker.Infrastructure.Configuration;
using Metria.EmailWorker.Infrastructure.Email;
using Metria.EmailWorker.Infrastructure.Messaging;
using Metria.EmailWorker.Infrastructure.Observability;
using Metria.EmailWorker.Infrastructure.Persistence;
using Metria.EmailWorker.Infrastructure.Persistence.Repositories;
using Metria.EmailWorker.Infrastructure.Time;

namespace Metria.EmailWorker.Infrastructure.DependencyInjection;

public static class InfrastructureServiceCollectionExtensions
{
    public static IServiceCollection AddEmailWorkerInfrastructure(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var sendGridConfigured = !string.IsNullOrWhiteSpace(configuration["SENDGRID_API_KEY"]);

        services
            .AddOptions<RabbitMqOptions>()
            .Configure(options =>
            {
                options.Host = configuration["RABBITMQ_HOST"] ?? string.Empty;
                options.Port = ParseInt(configuration["RABBITMQ_PORT"], 5672);
                options.User = configuration["RABBITMQ_USER"] ?? string.Empty;
                options.Password = configuration["RABBITMQ_PASSWORD"] ?? string.Empty;
                options.QueueEmailDigest = configuration["RABBITMQ_QUEUE_EMAIL_DIGEST"] ?? string.Empty;
            })
            .Validate(
                o => !string.IsNullOrWhiteSpace(o.Host) &&
                     o.Port > 0 &&
                     !string.IsNullOrWhiteSpace(o.User) &&
                     !string.IsNullOrWhiteSpace(o.Password) &&
                     !string.IsNullOrWhiteSpace(o.QueueEmailDigest),
                "RabbitMQ env variables are invalid.")
            .ValidateOnStart();

        services
            .AddOptions<SmtpOptions>()
            .Configure(options =>
            {
                options.Host = configuration["SMTP_HOST"] ?? string.Empty;
                options.Port = ParseInt(configuration["SMTP_PORT"], 587);
                options.User = configuration["SMTP_USER"] ?? string.Empty;
                options.Password = configuration["SMTP_PASSWORD"] ?? string.Empty;
                options.From = configuration["SMTP_FROM"] ?? string.Empty;
            })
            .Validate(
                o => sendGridConfigured || (
                    o.Port > 0 &&
                    !string.IsNullOrWhiteSpace(o.Host) &&
                    !string.IsNullOrWhiteSpace(o.From)),
                "SMTP configuration is invalid when SendGrid is not configured.")
            .ValidateOnStart();

        services
            .AddOptions<SendGridOptions>()
            .Configure(options =>
            {
                options.ApiKey = configuration["SENDGRID_API_KEY"] ?? string.Empty;
                options.From = configuration["SENDGRID_FROM"] ?? string.Empty;
            })
            .Validate(
                o => string.IsNullOrWhiteSpace(o.ApiKey) || !string.IsNullOrWhiteSpace(o.From),
                "SENDGRID_FROM must be set when SENDGRID_API_KEY is configured.")
            .ValidateOnStart();

        services
            .AddOptions<RetryOptions>()
            .Configure(options =>
            {
                options.MaxAttempts = ParseInt(configuration["EMAIL_RETRY_MAX_ATTEMPTS"], 5);
                options.BaseDelaySeconds = ParseInt(configuration["EMAIL_RETRY_BASE_DELAY_SECONDS"], 2);
                options.MaxDelaySeconds = ParseInt(configuration["EMAIL_RETRY_MAX_DELAY_SECONDS"], 60);
            })
            .Validate(
                o => o.MaxAttempts >= 1 && o.BaseDelaySeconds >= 1 && o.MaxDelaySeconds >= o.BaseDelaySeconds,
                "Retry configuration is invalid.")
            .ValidateOnStart();

        services
            .AddOptions<SchedulerOptions>()
            .Configure(options =>
            {
                options.Enabled = ParseBool(configuration["EMAIL_DIGEST_ENABLED"]);
                options.Cron = configuration["EMAIL_DIGEST_CRON"] ?? string.Empty;
                options.TimeZoneDefault = configuration["EMAIL_DIGEST_TIMEZONE_DEFAULT"] ?? string.Empty;
            })
            .Validate(
                o => !o.Enabled || (!string.IsNullOrWhiteSpace(o.Cron) && !string.IsNullOrWhiteSpace(o.TimeZoneDefault)),
                "Scheduler env variables are invalid when EMAIL_DIGEST_ENABLED=true.")
            .Validate(
                o => !o.Enabled || IsValidTimeZone(o.TimeZoneDefault),
                "EMAIL_DIGEST_TIMEZONE_DEFAULT is invalid.")
            .ValidateOnStart();

        var connectionString = configuration["ConnectionStrings:EmailWorkerDb"]
                               ?? configuration["ConnectionStrings__EmailWorkerDb"]
                               ?? string.Empty;

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException("ConnectionStrings__EmailWorkerDb must be configured.");
        }

        services.AddDbContext<EmailWorkerDbContext>(optionsBuilder =>
            optionsBuilder.UseNpgsql(connectionString));

        services.AddScoped<IEmailDispatchRepository, EmailDispatchRepository>();
        services.AddScoped<ProcessEmailDigestUseCase>();
        services.AddSingleton<EmailDigestMessageValidator>();
        services.AddSingleton<IClock, SystemClock>();
        services.AddSingleton<IEmailTemplateRenderer, EmailTemplateRenderer>();
        services.AddSingleton<IEmailWorkerMetrics, EmailWorkerMetrics>();
        services.AddSingleton<RabbitMqMessageDeserializer>();
        services.AddSingleton<RabbitMqConsumer>();

        services.AddSingleton<IEmailSender>(serviceProvider =>
        {
            var sendGridOptions = serviceProvider
                .GetRequiredService<Microsoft.Extensions.Options.IOptions<SendGridOptions>>()
                .Value;

            if (sendGridOptions.IsConfigured)
            {
                return new SendGridEmailSender(
                    serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<SendGridOptions>>());
            }

            return new SmtpEmailSender(
                serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<SmtpOptions>>());
        });

        return services;
    }

    private static int ParseInt(string? value, int defaultValue) =>
        int.TryParse(value, out var parsed) ? parsed : defaultValue;

    private static bool ParseBool(string? value) =>
        bool.TryParse(value, out var parsed) && parsed;

    private static bool IsValidTimeZone(string value)
    {
        try
        {
            _ = TimeZoneInfo.FindSystemTimeZoneById(value);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
