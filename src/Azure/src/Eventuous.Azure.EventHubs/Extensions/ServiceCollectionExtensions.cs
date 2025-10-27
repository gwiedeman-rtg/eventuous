// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Eventuous.Producers;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Checkpoints;
using Eventuous.Subscriptions.Filters;
using Eventuous.Azure.EventHubs.Subscriptions;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs.Consumer;
using Microsoft.Extensions.Logging;

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

        // Register TableServiceClient if atomic versioning is enabled and table storage is configured
        services.AddSingleton<TableServiceClient>(serviceProvider => {
            var options = serviceProvider.GetRequiredService<IOptions<AzureEventHubsEventStoreOptions>>().Value;
            if (options.EnableAtomicVersioning && !string.IsNullOrWhiteSpace(options.TableStorageConnectionString)) {
                return new TableServiceClient(options.TableStorageConnectionString);
            }
            return null!; // Will be handled in the event store constructor
        });

        services.AddSingleton<IEventStore>(serviceProvider => {
            var options = serviceProvider.GetRequiredService<IOptions<AzureEventHubsEventStoreOptions>>().Value;
            options.Validate();

            var serializer = serviceProvider.GetService<IEventSerializer>();
            var metaSerializer = serviceProvider.GetService<IMetadataSerializer>();
            var logger = serviceProvider.GetService<ILogger<AzureEventHubsEventStore>>();
            var loggerFactory = serviceProvider.GetService<ILoggerFactory>();
            var tableServiceClient = serviceProvider.GetService<TableServiceClient>();

            return new AzureEventHubsEventStore(
                new EventHubProducerClient(options.EventHubConnectionString, options.EventHubName),
                new EventHubConsumerClient(options.ConsumerGroup, options.EventHubConnectionString, options.EventHubName),
                new BlobServiceClient(options.BlobStorageConnectionString),
                options.EventHubName,
                options.CaptureContainerName,
                options.UseRealtimeReading,
                serializer,
                metaSerializer,
                logger,
                loggerFactory,
                tableServiceClient,
                options.EnableAtomicVersioning,
                options.VersionLockContainerName
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
    /// <param name="tableServiceClient">Optional table service client for atomic versioning</param>
    /// <param name="enableAtomicVersioning">Whether to enable atomic version control</param>
    /// <param name="versionLockContainer">Container name for blob lease versioning</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddAzureEventHubsEventStore(
            this IServiceCollection services,
            EventHubProducerClient  producerClient,
            EventHubConsumerClient  consumerClient,
            BlobServiceClient       blobServiceClient,
            string                  eventHubName,
            string                  captureContainerName,
            bool                    useRealtimeReading = true,
            TableServiceClient?     tableServiceClient = null,
            bool                   enableAtomicVersioning = false,
            string?                versionLockContainer = null
        ) {
        services.AddSingleton<IEventStore>(serviceProvider => {
            var serializer = serviceProvider.GetService<IEventSerializer>();
            var metaSerializer = serviceProvider.GetService<IMetadataSerializer>();
            var logger = serviceProvider.GetService<ILogger<AzureEventHubsEventStore>>();
            var loggerFactory = serviceProvider.GetService<ILoggerFactory>();

            return new AzureEventHubsEventStore(
                producerClient,
                consumerClient,
                blobServiceClient,
                eventHubName,
                captureContainerName,
                useRealtimeReading,
                serializer,
                metaSerializer,
                logger,
                loggerFactory,
                tableServiceClient,
                enableAtomicVersioning,
                versionLockContainer
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

    /// <summary>
    /// Add Azure Event Hubs Subscription to the service collection
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="subscriptionId">Subscription ID</param>
    /// <param name="configureOptions">Configuration action for options</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddAzureEventHubsSubscription(
            this IServiceCollection services,
            string subscriptionId,
            Action<AzureEventHubsSubscriptionOptions> configureOptions
        ) {
        services.Configure(configureOptions);

        services.AddSingleton<IMessageSubscription>(serviceProvider => {
            var options = serviceProvider.GetRequiredService<IOptions<AzureEventHubsSubscriptionOptions>>().Value;
            options.SubscriptionId = subscriptionId;
            options.Validate();

            var checkpointStore = serviceProvider.GetRequiredService<ICheckpointStore>();
            var consumePipe = serviceProvider.GetRequiredService<ConsumePipe>();
            var loggerFactory = serviceProvider.GetService<ILoggerFactory>();
            var eventSerializer = serviceProvider.GetService<IEventSerializer>();
            var metaSerializer = serviceProvider.GetService<IMetadataSerializer>();

            return new AzureEventHubsSubscription(
                options,
                checkpointStore,
                consumePipe,
                loggerFactory,
                eventSerializer,
                metaSerializer
            );
        });

        return services;
    }
}