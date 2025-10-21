using Bookings.Payments.Application;
using Bookings.Payments.Domain;
using Bookings.Payments.Infrastructure;
using Bookings.Payments.Integration;
using Eventuous.Diagnostics.OpenTelemetry;
using Eventuous.Azure.ServiceBus;
using Eventuous.Azure.ServiceBus.Producers;
using Eventuous.Azure.ServiceBus.Subscriptions;
using Eventuous.Projections.MongoDB;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Bookings.Payments;

public static class Registrations {
    public static void AddServices(this IServiceCollection services, IConfiguration configuration) {
        // Azure Service Bus setup
        services.AddServiceBusProducer(options => {
            options.ConnectionString = configuration["ServiceBus:ConnectionString"]!;
            options.TopicName = configuration["ServiceBus:TopicName"]!;
        });
        services.AddCommandService<CommandService, PaymentState>();
        services.AddSingleton(Mongo.ConfigureMongo(configuration));
        services.AddCheckpointStore<MongoCheckpointStore>();

        services
            .AddGateway<ServiceBusSubscription, ServiceBusSubscriptionOptions, ServiceBusProducer, ServiceBusProduceOptions>(
                "IntegrationSubscription",
                PaymentsGateway.Transform
            );
    }

    public static void AddTelemetry(this IServiceCollection services) {
        services.AddOpenTelemetry()
            .WithMetrics(
                builder => builder
                    .AddAspNetCoreInstrumentation()
                    .AddEventuous()
                    .AddEventuousSubscriptions()
                    .AddPrometheusExporter()
            );

        services.AddOpenTelemetry()
            .WithTracing(
                builder => builder
                    .AddAspNetCoreInstrumentation()
                    .AddGrpcClientInstrumentation()
                    .AddEventuousTracing()
                    .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("payments"))
                    .SetSampler(new AlwaysOnSampler())
                    .AddZipkinExporter()
            );
    }
}
