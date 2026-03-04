using Metria.EmailWorker.Infrastructure.DependencyInjection;
using Metria.EmailWorker.Processor.HealthChecks;
using Metria.EmailWorker.Processor.HostedServices;

namespace Metria.EmailWorker.Processor.Extensions;

public static class HostBuilderExtensions
{
    public static IServiceCollection AddEmailWorker(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddEmailWorkerInfrastructure(configuration);
        services.AddSingleton<EmailDigestMessageProcessor>();
        services.AddHostedService<EmailDigestConsumerHostedService>();
        services.AddHealthChecks()
            .AddCheck<RabbitMqHealthCheck>("rabbitmq")
            .AddCheck<PostgresHealthCheck>("postgres");

        return services;
    }
}
