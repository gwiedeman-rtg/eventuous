using System.Text.Json;
using Bookings.Application;
using Bookings.Application.Queries;
using Bookings.Domain;
using Bookings.Domain.Bookings;
using Bookings.Infrastructure;
using Bookings.Integration;
using Eventuous;
using Eventuous.Diagnostics.OpenTelemetry;
using Eventuous.Azure.EventHubs;
using Eventuous.Azure.EventHubs.Subscriptions;
using Eventuous.Azure.ServiceBus.Subscriptions;
using Eventuous.Projections.MongoDB;
using Eventuous.Subscriptions.Registrations;
using MongoDB.Driver.Core.Extensions.DiagnosticSources;
using NodaTime;
using NodaTime.Serialization.SystemTextJson;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Bookings;

public static class Registrations {
    public static void AddEventuous(this IServiceCollection services, IConfiguration configuration) {
        DefaultEventSerializer.SetDefaultSerializer(
            new DefaultEventSerializer(new JsonSerializerOptions(JsonSerializerDefaults.Web).ConfigureForNodaTime(DateTimeZoneProviders.Tzdb))
        );

        // Azure Event Hubs Event Store
        services.AddAzureEventHubsEventStore(options => {
            options.EventHubConnectionString = configuration["AzureEventHubs:ConnectionString"]!;
            options.EventHubName = configuration["AzureEventHubs:EventHubName"]!;
            options.BlobStorageConnectionString = configuration["AzureBlobStorage:ConnectionString"]!;
            options.TableStorageConnectionString = configuration["AzureTableStorage:ConnectionString"]!;
            options.CaptureContainerName = configuration["AzureBlobStorage:CaptureContainer"]!;
        });
        services.AddCommandService<BookingsCommandService, BookingState>();

        services.AddSingleton<Services.IsRoomAvailable>((_,    _) => new(true));
        services.AddSingleton<Services.ConvertCurrency>((from, currency) => new Money(from.Amount * 2, currency));

        services.AddSingleton(Mongo.ConfigureMongo(configuration));

        // Azure Event Hubs subscription for projections
        services.AddSubscription<AzureEventHubsSubscription, AzureEventHubsSubscriptionOptions>(
            "BookingsProjections",
            builder => builder
                .UseCheckpointStore<MongoCheckpointStore>()
                .AddEventHandler<BookingStateProjection>()
                .AddEventHandler<MyBookingsProjection>()
                .WithPartitioningByStream(2)
        );
        services.AddSingleton<BookingsQueryService>();

        // Azure Service Bus subscription for payment integration
        services.AddSubscription<ServiceBusSubscription, ServiceBusSubscriptionOptions>(
            "PaymentIntegration",
            builder => builder
                .Configure(x => x.TopicName = PaymentsIntegrationHandler.Stream)
                .AddEventHandler<PaymentsIntegrationHandler>()
        );
    }

    public static void AddTelemetry(this IServiceCollection services) {
        var otelEnabled = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT") != null;

        services.AddOpenTelemetry()
            .WithMetrics(
                builder => {
                    builder
                        .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("bookings"))
                        .AddAspNetCoreInstrumentation()
                        .AddEventuous()
                        .AddEventuousSubscriptions()
                        .AddPrometheusExporter();
                    if (otelEnabled) builder.AddOtlpExporter();
                }
            );

        services.AddOpenTelemetry()
            .WithTracing(
                builder => {
                    builder
                        .AddAspNetCoreInstrumentation()
                        .AddGrpcClientInstrumentation()
                        .AddEventuousTracing()
                        .AddSource(typeof(DiagnosticsActivityEventSubscriber).Assembly.GetName().Name!)
                        .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("bookings"))
                        .SetSampler(new AlwaysOnSampler());

                    if (otelEnabled)
                        builder.AddOtlpExporter();
                    else
                        builder.AddZipkinExporter();
                }
            );
    }
}
