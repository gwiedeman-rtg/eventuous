// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Runtime.CompilerServices;
using System.Text.Json;
using Eventuous.Tools;
using static Eventuous.DeserializationResult;

namespace Eventuous.Azure.EventHubs;

/// <summary>
/// Azure Event Hubs consumer for reading events directly from Event Hubs
/// </summary>
public class AzureEventHubsConsumer : IDisposable {
    readonly EventHubConsumerClient             _consumerClient;
    readonly IEventSerializer                   _serializer;
    readonly IMetadataSerializer                _metaSerializer;
    readonly ILogger<AzureEventHubsConsumer>?   _logger;
    readonly CancellationTokenSource            _cancellationTokenSource;

    bool _disposed;

    /// <summary>
    /// Initialize the consumer with the given Event Hub consumer client
    /// </summary>
    /// <param name="consumerClient">Event Hub consumer client instance</param>
    /// <param name="serializer">Optional event serializer. When not provided, the default serializer will be used.</param>
    /// <param name="metaSerializer">Optional metadata serializer. When not provided, the default serializer will be used.</param>
    /// <param name="logger">Optional logger</param>
    public AzureEventHubsConsumer(
            EventHubConsumerClient             consumerClient,
            IEventSerializer?                  serializer     = null,
            IMetadataSerializer?               metaSerializer = null,
            ILogger<AzureEventHubsConsumer>?   logger         = null
        ) {
        _consumerClient           = Ensure.NotNull(consumerClient);
        _serializer               = serializer     ?? DefaultEventSerializer.Instance;
        _metaSerializer           = metaSerializer ?? DefaultMetadataSerializer.Instance;
        _logger                   = logger;
        _cancellationTokenSource  = new CancellationTokenSource();
    }

    /// <summary>
    /// Initialize the consumer with connection string
    /// </summary>
    /// <param name="connectionString">Event Hub connection string</param>
    /// <param name="eventHubName">Event Hub name</param>
    /// <param name="consumerGroup">Consumer group name</param>
    /// <param name="serializer">Optional event serializer. When not provided, the default serializer will be used.</param>
    /// <param name="metaSerializer">Optional metadata serializer. When not provided, the default serializer will be used.</param>
    /// <param name="logger">Optional logger</param>
    public AzureEventHubsConsumer(
            string                             connectionString,
            string                             eventHubName,
            string                             consumerGroup,
            IEventSerializer?                  serializer     = null,
            IMetadataSerializer?               metaSerializer = null,
            ILogger<AzureEventHubsConsumer>?   logger         = null
        ) : this(
            new EventHubConsumerClient(
                Ensure.NotEmptyString(consumerGroup),
                Ensure.NotEmptyString(connectionString),
                Ensure.NotEmptyString(eventHubName)
            ),
            serializer,
            metaSerializer,
            logger
        ) { }

    /// <summary>
    /// Read events from a specific stream starting from a given position
    /// </summary>
    /// <param name="stream">Stream name to read from</param>
    /// <param name="startPosition">Position to start reading from</param>
    /// <param name="maxEvents">Maximum number of events to read</param>
    /// <param name="timeout">Timeout for the read operation</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Array of stream events</returns>
    public async Task<StreamEvent[]> ReadEventsFromStream(
            StreamName        stream,
            EventPosition     startPosition,
            int               maxEvents = 100,
            TimeSpan?         timeout = null,
            CancellationToken cancellationToken = default
        ) {
        var events = new List<StreamEvent>();
        var readTimeout = timeout ?? TimeSpan.FromSeconds(30);

        try {
            using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                _cancellationTokenSource.Token
            );

            combinedCts.CancelAfter(readTimeout);

            var readOptions = new ReadEventOptions {
                MaximumWaitTime = TimeSpan.FromSeconds(1)
            };

            // Read from all partitions to find events for this stream
            var partitionIds = await _consumerClient.GetPartitionIdsAsync(combinedCts.Token).NoContext();

            var readTasks = partitionIds.Select(async partitionId => {
                var partitionEvents = new List<StreamEvent>();

                await foreach (var partitionEvent in _consumerClient.ReadEventsFromPartitionAsync(
                    partitionId,
                    startPosition,
                    readOptions,
                    combinedCts.Token
                )) {
                    if (partitionEvents.Count >= maxEvents) break;

                    var streamEvent = ConvertToStreamEvent(partitionEvent, stream);
                    if (streamEvent != null) {
                        partitionEvents.Add(streamEvent.Value);
                    }
                }

                return partitionEvents;
            });

            var allPartitionEvents = await Task.WhenAll(readTasks).NoContext();

            // Combine and sort events by sequence number
            events = allPartitionEvents
                .SelectMany(x => x)
                .OrderBy(x => x.Position)
                .Take(maxEvents)
                .ToList();

        } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
            throw;
        } catch (OperationCanceledException) {
            // Timeout occurred
            _logger?.LogWarning("Timeout occurred while reading events from stream {Stream}", stream);
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to read events from stream {Stream}", stream);
            throw;
        }

        return events.ToArray();
    }

    /// <summary>
    /// Read events from all partitions starting from a given position
    /// </summary>
    /// <param name="startPosition">Position to start reading from</param>
    /// <param name="maxEvents">Maximum number of events to read</param>
    /// <param name="timeout">Timeout for the read operation</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Array of stream events</returns>
    public async Task<StreamEvent[]> ReadAllEvents(
            EventPosition     startPosition,
            int               maxEvents = 100,
            TimeSpan?         timeout = null,
            CancellationToken cancellationToken = default
        ) {
        var events = new List<StreamEvent>();
        var readTimeout = timeout ?? TimeSpan.FromSeconds(30);

        try {
            using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                _cancellationTokenSource.Token
            );

            combinedCts.CancelAfter(readTimeout);

            var readOptions = new ReadEventOptions {
                MaximumWaitTime = TimeSpan.FromSeconds(1)
            };

            // Read from all partitions
            var partitionIds = await _consumerClient.GetPartitionIdsAsync(combinedCts.Token).NoContext();

            var readTasks = partitionIds.Select(async partitionId => {
                var partitionEvents = new List<StreamEvent>();

                await foreach (var partitionEvent in _consumerClient.ReadEventsFromPartitionAsync(
                    partitionId,
                    startPosition,
                    readOptions,
                    combinedCts.Token
                )) {
                    if (partitionEvents.Count >= maxEvents) break;

                    var streamEvent = ConvertToStreamEvent(partitionEvent);
                    if (streamEvent != null) {
                        partitionEvents.Add(streamEvent.Value);
                    }
                }

                return partitionEvents;
            });

            var allPartitionEvents = await Task.WhenAll(readTasks).NoContext();

            // Combine and sort events by sequence number
            events = allPartitionEvents
                .SelectMany(x => x)
                .OrderBy(x => x.Position)
                .Take(maxEvents)
                .ToList();

        } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
            throw;
        } catch (OperationCanceledException) {
            // Timeout occurred
            _logger?.LogWarning("Timeout occurred while reading all events");
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to read all events");
            throw;
        }

        return events.ToArray();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    StreamEvent? ConvertToStreamEvent(PartitionEvent partitionEvent, StreamName? targetStream = null) {
        try {
            var eventData = partitionEvent.Data;

            if (eventData == null) {
                _logger?.LogWarning("partitionEvent.Data is null");
                return null;
            }

            _logger?.LogDebug("Converting partition event: MessageId={MessageId}, BodyLength={BodyLength}",
                eventData.MessageId, eventData.EventBody.IsEmpty ? 0 : eventData.EventBody.Length);

            // Check if this event belongs to the target stream (if specified)
            if (targetStream != null) {
                if (!eventData.Properties.TryGetValue("StreamName", out var streamNameObj) ||
                    streamNameObj?.ToString() != targetStream.ToString()) {
                    _logger?.LogDebug("Event belongs to different stream: Expected={Expected}, Actual={Actual}",
                        targetStream, streamNameObj?.ToString() ?? "null");
                    return null;
                }
            }

            // Extract event type
            if (!eventData.Properties.TryGetValue("EventType", out var eventTypeObj) ||
                eventTypeObj?.ToString() is not { } eventType) {
                return null;
            }

            // Deserialize the event
            var deserialized = _serializer.DeserializeEvent(
                eventData.EventBody.ToArray(),
                eventType,
                eventData.ContentType ?? "application/json"
            );

            if (deserialized is not SuccessfullyDeserialized success) {
                _logger?.LogWarning("Failed to deserialize event of type {EventType}", eventType);
                return null;
            }

            // Extract metadata
            Metadata? metadata = null;
            if (eventData.Properties.TryGetValue("Metadata", out var metadataObj) &&
                metadataObj?.ToString() is { } metadataBase64) {
                try {
                    var metadataBytes = Convert.FromBase64String(metadataBase64);
                    metadata = _metaSerializer.Deserialize(metadataBytes);
                } catch (Exception ex) {
                    _logger?.LogWarning(ex, "Failed to deserialize metadata for event {MessageId}", eventData.MessageId);
                }
            }

            // Extract event ID
            var eventId = Guid.NewGuid(); // Fallback
            if (!string.IsNullOrEmpty(eventData.MessageId)) {
                Guid.TryParse(eventData.MessageId, out eventId);
            }

            return new StreamEvent(
                eventId,
                success.Payload,
                metadata ?? new Metadata(),
                eventData.ContentType ?? "application/json",
                partitionEvent.Data.SequenceNumber
            );
        } catch (Exception ex) {
            _logger?.LogWarning(ex, "Failed to convert partition event to stream event");
            return null;
        }
    }

    public void Dispose() {
        if (_disposed) return;

        _cancellationTokenSource.Cancel();
        _cancellationTokenSource.Dispose();
        _consumerClient.DisposeAsync().GetAwaiter().GetResult();
        _disposed = true;
    }
}