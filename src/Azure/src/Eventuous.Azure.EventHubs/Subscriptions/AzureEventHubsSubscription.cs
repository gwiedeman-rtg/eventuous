// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Text;
using System.Text.Json;
using Azure.Messaging.EventHubs.Processor;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Checkpoints;
using Eventuous.Subscriptions.Context;
using Eventuous.Subscriptions.Filters;
using Microsoft.Extensions.Logging;
using Eventuous.Diagnostics;
using Eventuous.Diagnostics.Tracing;
using static Eventuous.DeserializationResult;
using Azure.Storage.Blobs;

namespace Eventuous.Azure.EventHubs.Subscriptions;

/// <summary>
/// Azure Event Hubs subscription using EventProcessorClient for automatic checkpoint management
/// </summary>
public class AzureEventHubsSubscription : EventSubscription<AzureEventHubsSubscriptionOptions> {
    readonly EventProcessorClient _processorClient;
    readonly ILogger<AzureEventHubsSubscription>? _logger;

    public AzureEventHubsSubscription(
            AzureEventHubsSubscriptionOptions options,
            ICheckpointStore checkpointStore,
            ConsumePipe consumePipe,
            ILoggerFactory? loggerFactory = null,
            IEventSerializer? eventSerializer = null,
            IMetadataSerializer? metaSerializer = null
        ) : base(options, consumePipe, loggerFactory, eventSerializer) {
        _logger = loggerFactory?.CreateLogger<AzureEventHubsSubscription>();

        // Create EventProcessorClient with blob storage for checkpoint management
        var storageClient = new BlobServiceClient(options.BlobStorageConnectionString);
        var containerClient = storageClient.GetBlobContainerClient(options.CheckpointContainerName);

        _processorClient = new EventProcessorClient(
            containerClient,
            options.ConsumerGroup,
            options.EventHubConnectionString,
            options.EventHubName
        );

        // Configure event processing
        _processorClient.ProcessEventAsync += ProcessEventAsync;
        _processorClient.ProcessErrorAsync += ProcessErrorAsync;
    }

    protected override async ValueTask Subscribe(CancellationToken cancellationToken) {
        try {
            // Start processing events
            await _processorClient.StartProcessingAsync(cancellationToken).NoContext();
            _logger?.LogInformation("Started Azure Event Hubs subscription {SubscriptionId}", SubscriptionId);
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to start Azure Event Hubs subscription {SubscriptionId}", SubscriptionId);
            throw;
        }
    }

    protected override async ValueTask Unsubscribe(CancellationToken cancellationToken) {
        try {
            await _processorClient.StopProcessingAsync(cancellationToken).NoContext();
            _logger?.LogInformation("Stopped Azure Event Hubs subscription {SubscriptionId}", SubscriptionId);
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to stop Azure Event Hubs subscription {SubscriptionId}", SubscriptionId);
            throw;
        }
    }

    async Task ProcessEventAsync(ProcessEventArgs eventArgs) {
        if (eventArgs.CancellationToken.IsCancellationRequested) return;

        try {
            var eventData = eventArgs.Data;

            // Extract stream name from partition key or properties
            var streamName = GetStreamName(eventData);
            if (streamName == null) return;

            // Extract event type
            if (!eventData.Properties.TryGetValue("EventType", out var eventTypeObj) ||
                eventTypeObj?.ToString() is not { } eventType) {
                _logger?.LogWarning("Event missing EventType property");
                return;
            }

            // Extract stream position
            long streamPosition = 0;
            if (eventData.Properties.TryGetValue("StreamPosition", out var positionObj) &&
                positionObj?.ToString() is { } positionStr) {
                long.TryParse(positionStr, out streamPosition);
            }

            // Extract metadata
            Metadata? metadata = null;
            if (eventData.Properties.TryGetValue("Metadata", out var metadataObj) &&
                metadataObj?.ToString() is { } metadataBase64) {
                try {
                    var metadataBytes = Convert.FromBase64String(metadataBase64);
                    metadata = DefaultMetadataSerializer.Instance.Deserialize(metadataBytes);
                } catch (Exception ex) {
                    _logger?.LogWarning(ex, "Failed to deserialize metadata");
                }
            }

            // Create consume context
            var context = new MessageConsumeContext(
                eventData.MessageId ?? Guid.NewGuid().ToString(),
                eventType,
                eventData.ContentType ?? "application/json",
                streamName,
                (ulong)streamPosition,
                (ulong)streamPosition,
                (ulong)eventData.SequenceNumber,
                (ulong)eventData.SequenceNumber,
                eventData.EnqueuedTime.DateTime,
                eventData.EventBody.ToArray(),
                metadata ?? new Metadata(),
                Options.SubscriptionId,
                eventArgs.CancellationToken
            );

            // Process through the consume pipe
            await Handler(context).NoContext();

            // Update checkpoint
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken).NoContext();
        } catch (Exception ex) {
            _logger?.LogError(ex, "Error processing event in subscription {SubscriptionId}", SubscriptionId);
            throw;
        }
    }

    Task ProcessErrorAsync(ProcessErrorEventArgs eventArgs) {
        _logger?.LogError(eventArgs.Exception, "Error in Azure Event Hubs subscription {SubscriptionId}: {Error}",
            SubscriptionId, eventArgs.Exception.Message);

        // Mark subscription as dropped
        IsDropped = true;

        return Task.CompletedTask;
    }

    StreamName? GetStreamName(EventData eventData) {
        // First try to get from properties
        if (eventData.Properties.TryGetValue("StreamName", out var streamNameObj) &&
            streamNameObj?.ToString() is { } streamName) {
            return new StreamName(streamName);
        }

        // Fallback to partition key
        if (!string.IsNullOrEmpty(eventData.PartitionKey)) {
            return new StreamName(eventData.PartitionKey);
        }

        return null;
    }

    protected override async ValueTask Finalize(CancellationToken cancellationToken) {
        if (_processorClient != null) {
            await _processorClient.StopProcessingAsync(cancellationToken).NoContext();
            // EventProcessorClient implements IAsyncDisposable but DisposeAsync is not available in this version
            // The client will be disposed when the subscription is disposed
        }
    }
}
