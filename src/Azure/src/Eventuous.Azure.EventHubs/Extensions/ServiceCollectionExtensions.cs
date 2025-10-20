// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Eventuous.Producers;

namespace Eventuous.Azure.EventHubs.Extensions;

/// <summary>
/// Service collection extensions for Azure Event Hubs
/// </summary>
public static class ServiceCollectionExtensions {
    /// <summary>
    /// Add Azure Event Hubs Event Store to the service collection
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="configureOptions">Configuration action for options</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddAzureEventHubsEventStore(
            this IServiceCollection                           services,
            Action<AzureEventHubsEventStoreOptions>           configureOptions
        ) {
        services.Configure(configureOptions);
        
        services.AddSingleton<IEventStore>(serviceProvider => {
            var options = serviceProvider.GetRequiredService<IOptions<AzureEventHubsEventStoreOptions>>().Value;
            options.Validate();

            var serializer = serviceProvider.GetService<IEventSerializer>();
            var metaSerializer = serviceProvider.GetService<IMetadataSerializer>();
            var logger = serviceProvider.GetService<ILogger<AzureEventHubsEventStore>>();

            return new AzureEventHubsEventStore(
                options.EventHubConnectionString,
                options.EventHubName,
                options.BlobStorageConnectionString,
                options.CaptureContainerName,
                options.ConsumerGroup,
                options.UseRealtimeReading,
                serializer,
                metaSerializer,
                logger
            );
        });

        return services;
    }

    /// <summary>
    /// Add Azure Event Hubs Event Store with explicit clients
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="producerClient">Event Hub producer client</param>
    /// <param name="consumerClient">Event Hub consumer client</param>
    /// <param name="blobServiceClient">Blob service client</param>
    /// <param name="eventHubName">Event Hub name</param>
    /// <param name="captureContainerName">Capture container name</param>
    /// <param name="useRealtimeReading">Whether to use real-time reading from Event Hubs</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddAzureEventHubsEventStore(
            this IServiceCollection services,
            EventHubProducerClient  producerClient,
            EventHubConsumerClient  consumerClient,
            BlobServiceClient       blobServiceClient,
            string                  eventHubName,
            string                  captureContainerName,
            bool                    useRealtimeReading = true
        ) {
        services.AddSingleton<IEventStore>(serviceProvider => {
            var serializer = serviceProvider.GetService<IEventSerializer>();
            var metaSerializer = serviceProvider.GetService<IMetadataSerializer>();
            var logger = serviceProvider.GetService<ILogger<AzureEventHubsEventStore>>();

            return new AzureEventHubsEventStore(
                producerClient,
                consumerClient,
                blobServiceClient,
                eventHubName,
                captureContainerName,
                useRealtimeReading,
                serializer,
                metaSerializer,
                logger
            );
        });

        return services;
    }

    /// <summary>
    /// Add Azure Event Hubs Producer to the service collection
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="connectionString">Event Hub connection string</param>
    /// <param name="eventHubName">Event Hub name</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddAzureEventHubsProducer(
            this IServiceCollection services,
            string                  connectionString,
            string                  eventHubName
        ) {
        services.AddSingleton<IProducer<AzureEventHubsProduceOptions>>(serviceProvider => {
            var serializer = serviceProvider.GetService<IEventSerializer>();
            var metaSerializer = serviceProvider.GetService<IMetadataSerializer>();
            var logger = serviceProvider.GetService<ILogger<AzureEventHubsProducer>>();

            return new AzureEventHubsProducer(
                connectionString,
                eventHubName,
                serializer,
                metaSerializer,
                logger
            );
        });

        services.AddSingleton<IProducer>(serviceProvider =>
            serviceProvider.GetRequiredService<IProducer<AzureEventHubsProduceOptions>>()
        );

        return services;
    }

    /// <summary>
    /// Add Azure Event Hubs Producer with explicit client
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="producerClient">Event Hub producer client</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddAzureEventHubsProducer(
            this IServiceCollection services,
            EventHubProducerClient  producerClient
        ) {
        services.AddSingleton<IProducer<AzureEventHubsProduceOptions>>(serviceProvider => {
            var serializer = serviceProvider.GetService<IEventSerializer>();
            var metaSerializer = serviceProvider.GetService<IMetadataSerializer>();
            var logger = serviceProvider.GetService<ILogger<AzureEventHubsProducer>>();

            return new AzureEventHubsProducer(
                producerClient,
                serializer,
                metaSerializer,
                logger
            );
        });

        services.AddSingleton<IProducer>(serviceProvider =>
            serviceProvider.GetRequiredService<IProducer<AzureEventHubsProduceOptions>>()
        );

        return services;
    }
}