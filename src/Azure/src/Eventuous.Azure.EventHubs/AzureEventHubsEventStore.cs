// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using Azure;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Storage.Blobs;
using Azure.Data.Tables;
using Eventuous.Diagnostics;
using Eventuous.Diagnostics.Tracing;
using Eventuous.Producers;
using Microsoft.Extensions.Logging;
using Eventuous.Tools;
using static Eventuous.DeserializationResult;
using static Eventuous.Diagnostics.PersistenceEventSource;

namespace Eventuous.Azure.EventHubs;

/// <summary>
/// Azure Event Hubs implementation of <see cref="IEventStore"/> with Capture support
/// </summary>
public class AzureEventHubsEventStore : IEventStore,IDisposable {
    readonly ILogger<AzureEventHubsEventStore>? _logger;
    readonly EventHubProducerClient             _producerClient;
    readonly BlobServiceClient                  _blobServiceClient;
    readonly AzureEventHubsConsumer             _consumer;
    readonly IEventSerializer                   _serializer;
    readonly IMetadataSerializer                _metaSerializer;
    readonly string                             _eventHubName;
    readonly string                             _captureContainerName;
    readonly bool                               _useRealtimeReading;
    readonly ILoggerFactory?                    _loggerFactory;

    bool _disposed;

    /// <summary>
    /// Initialize the event store with Event Hub producer and Blob storage client for reading captured events
    /// </summary>
    /// <param name="producerClient">Event Hub producer client instance</param>
    /// <param name="consumerClient">Event Hub consumer client for real-time reading</param>
    /// <param name="blobServiceClient">Blob service client for reading captured events</param>
    /// <param name="eventHubName">Event Hub name</param>
    /// <param name="captureContainerName">Blob container name where captured events are stored</param>
    /// <param name="useRealtimeReading">Whether to use real-time reading from Event Hubs for recent events</param>
    /// <param name="serializer">Optional event serializer. When not provided, the default serializer will be used.</param>
    /// <param name="metaSerializer">Optional metadata serializer. When not provided, the default serializer will be used.</param>
    /// <param name="logger">Optional logger</param>
    /// <param name="loggerFactory"></param>
    public AzureEventHubsEventStore(
            EventHubProducerClient              producerClient,
            EventHubConsumerClient              consumerClient,
            BlobServiceClient                   blobServiceClient,
            string                              eventHubName,
            string                              captureContainerName,
            bool                                useRealtimeReading = true,
            IEventSerializer?                   serializer     = null,
            IMetadataSerializer?                metaSerializer = null,
            ILogger<AzureEventHubsEventStore>?  logger         = null,
            ILoggerFactory?                     loggerFactory = null
        ) {
        _logger               = logger;
        _producerClient       = Ensure.NotNull(producerClient);
        _blobServiceClient    = Ensure.NotNull(blobServiceClient);
        _eventHubName         = Ensure.NotEmptyString(eventHubName);
        _captureContainerName = Ensure.NotEmptyString(captureContainerName);
        _useRealtimeReading   = useRealtimeReading;
        _serializer           = serializer     ?? DefaultEventSerializer.Instance;
        _metaSerializer       = metaSerializer ?? DefaultMetadataSerializer.Instance;
        _loggerFactory        = loggerFactory;
        _consumer             = new AzureEventHubsConsumer(consumerClient, serializer, metaSerializer, loggerFactory?.CreateLogger<AzureEventHubsConsumer>());
    }

    /// <summary>
    /// Initialize the event store with connection strings
    /// </summary>
    /// <param name="eventHubConnectionString">Event Hub connection string</param>
    /// <param name="eventHubName">Event Hub name</param>
    /// <param name="blobStorageConnectionString">Blob storage connection string</param>
    /// <param name="captureContainerName">Blob container name where captured events are stored</param>
    /// <param name="consumerGroup">Consumer group for reading events (default: $Default)</param>
    /// <param name="useRealtimeReading">Whether to use real-time reading from Event Hubs for recent events</param>
    /// <param name="serializer">Optional event serializer. When not provided, the default serializer will be used.</param>
    /// <param name="metaSerializer">Optional metadata serializer. When not provided, the default serializer will be used.</param>
    /// <param name="logger">Optional logger</param>
    public AzureEventHubsEventStore(
            string                              eventHubConnectionString,
            string                              eventHubName,
            string                              blobStorageConnectionString,
            string                              captureContainerName,
            string                              consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName,
            bool                                useRealtimeReading = true,
            IEventSerializer?                   serializer     = null,
            IMetadataSerializer?                metaSerializer = null,
            ILogger<AzureEventHubsEventStore>?  logger         = null
        ) : this(
            new EventHubProducerClient(Ensure.NotEmptyString(eventHubConnectionString), Ensure.NotEmptyString(eventHubName)),
            new EventHubConsumerClient(consumerGroup, Ensure.NotEmptyString(eventHubConnectionString), Ensure.NotEmptyString(eventHubName)),
            new BlobServiceClient(Ensure.NotEmptyString(blobStorageConnectionString)),
            eventHubName,
            captureContainerName,
            useRealtimeReading,
            serializer,
            metaSerializer,
            logger
        ) { }

    /// <inheritdoc/>
    public async Task<bool> StreamExists(StreamName stream, CancellationToken cancellationToken = default) {
        try {
            // Check if any captured events exist for this stream by looking for blobs with the stream name prefix
            var containerClient = _blobServiceClient.GetBlobContainerClient(_captureContainerName);
            var blobPages = containerClient.GetBlobsAsync(prefix: GetBlobPrefix(stream), cancellationToken: cancellationToken)
                .AsPages(pageSizeHint: 1);

            await foreach (var page in blobPages) {
                if (page.Values.Any()) {
                    return true;
                }
                break; // Only check the first page
            }

            return false;
        } catch (Exception ex) {
            _logger?.LogWarning(ex, "Failed to check if stream {Stream} exists", stream);
            return false;
        }
    }

    /// <inheritdoc/>
    public async Task<AppendEventsResult> AppendEvents(
            StreamName                          stream,
            ExpectedStreamVersion               expectedVersion,
            IReadOnlyCollection<NewStreamEvent> events,
            CancellationToken                   cancellationToken = default
        ) {
        if (!events.Any()) {
            return AppendEventsResult.NoOp;
        }

        try {
            // Check expected version for optimistic concurrency
            var currentVersion = await GetCurrentStreamVersion(stream, cancellationToken).NoContext();

            // Handle NoStream case
            if (expectedVersion == ExpectedStreamVersion.NoStream) {
                if (currentVersion.HasValue) {
                    throw new AppendToStreamException(stream, new InvalidOperationException($"WrongExpectedVersion {-1}, stream already exists"));
                }
            }
            // Handle Any case - always allow
            else if (expectedVersion != ExpectedStreamVersion.Any) {
                // Stream exists or doesn't, check version match
                if (expectedVersion != ExpectedStreamVersion.NoStream) {
                    // Expected version is a specific number (0, 1, 2, ...)
                    if (!currentVersion.HasValue) {
                        throw new AppendToStreamException(stream, new InvalidOperationException($"WrongExpectedVersion {expectedVersion.Value}, stream doesn't exist"));
                    }
                    if (currentVersion.Value != expectedVersion.Value) {
                        throw new AppendToStreamException(stream, new InvalidOperationException($"WrongExpectedVersion {expectedVersion.Value}, current version {currentVersion.Value}"));
                    }
                }
            }

            var eventDataBatch = await _producerClient.CreateBatchAsync(cancellationToken).NoContext();
            var eventPosition = 0L;

            foreach (var streamEvent in events) {
                var eventData = ToEventData(streamEvent, stream);

                if (!eventDataBatch.TryAdd(eventData)) {
                    // If the batch is full, send it and create a new batch
                    await _producerClient.SendAsync(eventDataBatch, cancellationToken).NoContext();
                    eventDataBatch = await _producerClient.CreateBatchAsync(cancellationToken).NoContext();

                    if (!eventDataBatch.TryAdd(eventData)) {
                        throw new InvalidOperationException("Event is too large to fit in a batch");
                    }
                }
                eventPosition++;
            }

            // Send the final batch
            if (eventDataBatch.Count > 0) {
                await _producerClient.SendAsync(eventDataBatch, cancellationToken).NoContext();
            }

            // Event Hubs doesn't provide a global position like EventStore, so we use a timestamp-based approach
            var globalPosition = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var nextExpectedVersion = currentVersion.HasValue ? currentVersion.Value + events.Count : events.Count - 1;

            return new AppendEventsResult(globalPosition, nextExpectedVersion);
        } catch (AppendToStreamException) {
            throw;
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to append {Count} events to stream {Stream}", events.Count, stream);
            throw new AppendToStreamException(stream, ex);
        }
    }

    /// <inheritdoc/>
    public async Task<StreamEvent[]> ReadEvents(
            StreamName        stream,
            StreamReadPosition start,
            int               count,
            bool              failIfNotFound,
            CancellationToken cancellationToken = default
        ) {
        try {
            var events = new List<StreamEvent>();

            // First, try to read from real-time Event Hubs if enabled
            if (_useRealtimeReading) {
                try {
                    var realtimeEvents = await _consumer.ReadEventsFromStream(
                        stream,
                        EventPosition.Earliest,
                        count,
                        TimeSpan.FromSeconds(5),
                        cancellationToken
                    ).NoContext();

                    events.AddRange(realtimeEvents.Skip((int)start.Value).Take(count));

                    if (events.Count >= count) {
                        return events.ToArray();
                    }
                } catch (Exception ex) {
                    _logger?.LogWarning(ex, "Failed to read real-time events from stream {Stream}, falling back to captured events", stream);
                }
            }

            // If we don't have enough events from real-time, read from captured events
            var remainingCount = count - events.Count;
            if (remainingCount > 0) {
                var capturedEvents = await ReadEventsFromCapture(stream, start, remainingCount, cancellationToken).NoContext();
                events.AddRange(capturedEvents);
            }

            if (!events.Any() && failIfNotFound) {
                throw new StreamNotFound(stream);
            }

            return events.Take(count).ToArray();
        } catch (StreamNotFound) {
            throw;
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to read {Count} events from stream {Stream} starting at {Start}", count, stream, start);

            if (failIfNotFound) {
                throw new ReadFromStreamException(stream, ex);
            }

            return [];
        }
    }

    /// <inheritdoc/>
    public async Task<StreamEvent[]> ReadEventsBackwards(
            StreamName        stream,
            StreamReadPosition start,
            int               count,
            bool              failIfNotFound,
            CancellationToken cancellationToken = default
        ) {
        try {
            var allEvents = new List<StreamEvent>();
            var containerClient = _blobServiceClient.GetBlobContainerClient(_captureContainerName);

            // Get captured event blobs for this stream
            var blobPrefix = GetBlobPrefix(stream);
            var blobs = containerClient.GetBlobsAsync(prefix: blobPrefix, cancellationToken: cancellationToken);

            // Collect all events first
            await foreach (var blobItem in blobs) {
                var blobClient = containerClient.GetBlobClient(blobItem.Name);
                var streamEvents = await ReadEventsFromBlob(blobClient, stream, cancellationToken).NoContext();
                allEvents.AddRange(streamEvents);
            }

            if (!allEvents.Any() && failIfNotFound) {
                throw new StreamNotFound(stream);
            }

            // Sort by position and take from the end
            var sortedEvents = allEvents.OrderBy(e => e.Position).ToArray();
            var startIndex = start.Value == long.MaxValue ? sortedEvents.Length - 1 : (int)start.Value;
            var result = new List<StreamEvent>();

            for (var i = Math.Min(startIndex, sortedEvents.Length - 1); i >= 0 && result.Count < count; i--) {
                result.Add(sortedEvents[i]);
            }

            return result.ToArray();
        } catch (StreamNotFound) {
            throw;
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to read {Count} events backwards from stream {Stream} starting at {Start}", count, stream, start);

            if (failIfNotFound) {
                throw new ReadFromStreamException(stream, ex);
            }

            return [];
        }
    }

    /// <inheritdoc/>
    public Task TruncateStream(
            StreamName             stream,
            StreamTruncatePosition truncatePosition,
            ExpectedStreamVersion  expectedVersion,
            CancellationToken      cancellationToken = default
        ) {
        // Event Hubs with Capture doesn't support stream truncation
        // This is a limitation of the Event Hubs model
        throw new NotSupportedException("Stream truncation is not supported by Azure Event Hubs");
    }

    /// <inheritdoc/>
    public Task DeleteStream(
            StreamName            stream,
            ExpectedStreamVersion expectedVersion,
            CancellationToken     cancellationToken = default
        ) {
        // Event Hubs with Capture doesn't support stream deletion
        // This is a limitation of the Event Hubs model
        throw new NotSupportedException("Stream deletion is not supported by Azure Event Hubs");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    EventData ToEventData(NewStreamEvent streamEvent, StreamName stream) {
        var (eventType, contentType, payload) = _serializer.SerializeEvent(streamEvent.Payload!);
        var metadata = _metaSerializer.Serialize(streamEvent.Metadata);

        var eventData = new EventData(payload) {
            MessageId = streamEvent.Id.ToString(),
            ContentType = contentType
        };

        // Add custom properties for event type and metadata
        eventData.Properties["EventType"] = eventType;
        eventData.Properties["StreamName"] = stream.ToString();

        if (metadata.Length > 0) {
            eventData.Properties["Metadata"] = Convert.ToBase64String(metadata);
        }

        return eventData;
    }

    async Task<StreamEvent[]> ReadEventsFromCapture(
            StreamName        stream,
            StreamReadPosition start,
            int               count,
            CancellationToken cancellationToken
        ) {
        var events = new List<StreamEvent>();
        var containerClient = _blobServiceClient.GetBlobContainerClient(_captureContainerName);

        // Get captured event blobs for this stream
        var blobPrefix = GetBlobPrefix(stream);
        var blobs = containerClient.GetBlobsAsync(prefix: blobPrefix, cancellationToken: cancellationToken);

        var processedEvents = 0;
        var skippedEvents = 0;

        await foreach (var blobItem in blobs) {
            if (events.Count >= count) break;

            var blobClient = containerClient.GetBlobClient(blobItem.Name);
            var streamEvents = await ReadEventsFromBlob(blobClient, stream, cancellationToken).NoContext();

            foreach (var streamEvent in streamEvents) {
                if (skippedEvents < start.Value) {
                    skippedEvents++;
                    continue;
                }

                if (events.Count >= count) break;

                events.Add(streamEvent with { Position = processedEvents });
                processedEvents++;
            }
        }

        return events.ToArray();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    string GetBlobPrefix(StreamName stream) {
        // Azure Event Hubs Capture creates blobs with a specific naming pattern
        // We'll use the stream name to filter blobs
        return $"{_eventHubName}/";
    }

    async Task<List<StreamEvent>> ReadEventsFromBlob(BlobClient blobClient, StreamName stream, CancellationToken cancellationToken) {
        var events = new List<StreamEvent>();

        try {
            var response = await blobClient.DownloadContentAsync(cancellationToken).NoContext();
            var content = response.Value.Content.ToString();

            // Parse AVRO format used by Event Hubs Capture
            // This is a simplified implementation - in production, you'd use proper AVRO parsing
            var lines = content.Split('\n', StringSplitOptions.RemoveEmptyEntries);

            foreach (var line in lines) {
                try {
                    var eventData = ParseCapturedEvent(line, stream);
                    if (eventData != null) {
                        events.Add(eventData.Value);
                    }
                } catch (Exception ex) {
                    _logger?.LogWarning(ex, "Failed to parse captured event from blob {BlobName}", blobClient.Name);
                }
            }
        } catch (Exception ex) {
            _logger?.LogWarning(ex, "Failed to read events from blob {BlobName}", blobClient.Name);
        }

        return events;
    }

    StreamEvent? ParseCapturedEvent(string eventLine, StreamName targetStream) {
        try {
            // This is a simplified parser for demonstration
            // In production, you'd use proper AVRO deserialization
            var eventDoc = JsonDocument.Parse(eventLine);
            var root = eventDoc.RootElement;

            // Extract properties from the captured event
            if (!root.TryGetProperty("Properties", out var properties)) return null;

            if (!properties.TryGetProperty("StreamName", out var streamNameProp)) return null;
            var streamName = streamNameProp.GetString();

            if (streamName != targetStream.ToString()) return null;

            if (!properties.TryGetProperty("EventType", out var eventTypeProp)) return null;
            var eventType = eventTypeProp.GetString()!;

            if (!root.TryGetProperty("Body", out var bodyProp)) return null;
            var bodyBytes = Convert.FromBase64String(bodyProp.GetString()!);

            if (!root.TryGetProperty("ContentType", out var contentTypeProp)) return null;
            var contentType = contentTypeProp.GetString()!;

            // Deserialize the event
            var deserialized = _serializer.DeserializeEvent(bodyBytes, eventType, contentType);

            if (deserialized is not SuccessfullyDeserialized success) return null;

            // Extract metadata
            Metadata? metadata = null;
            if (properties.TryGetProperty("Metadata", out var metadataProp)) {
                var metadataBytes = Convert.FromBase64String(metadataProp.GetString()!);
                metadata = _metaSerializer.Deserialize(metadataBytes);
            }

            // Extract event ID
            var eventId = Guid.NewGuid(); // Fallback
            if (root.TryGetProperty("MessageId", out var messageIdProp)) {
                Guid.TryParse(messageIdProp.GetString(), out eventId);
            }

            return new StreamEvent(
                eventId,
                success.Payload,
                metadata ?? new Metadata(),
                contentType,
                0 // Position will be set by the caller
            );
        } catch (Exception ex) {
            _logger?.LogWarning(ex, "Failed to parse captured event: {EventLine}", eventLine);
            return null;
        }
    }

    /// <summary>
    /// Gets the current version of the stream by reading events and counting them
    /// Note: This is not atomic and may miss recent events, but it's the best we can do with Event Hubs
    /// Version is 0-indexed: 0 = first event, 1 = second event, etc.
    /// </summary>
    async Task<long?> GetCurrentStreamVersion(StreamName stream, CancellationToken cancellationToken) {
        try {
            // Try to read from real-time first
            if (_useRealtimeReading) {
                try {
                    var realtimeEvents = await _consumer.ReadEventsFromStream(
                        stream,
                        EventPosition.Earliest,
                        1000,
                        TimeSpan.FromSeconds(2),
                        cancellationToken
                    ).NoContext();

                    if (realtimeEvents.Length > 0) {
                        // Version is 0-indexed: 1 event → version 0
                        return realtimeEvents.Length - 1;
                    }
                } catch (Exception ex) {
                    _logger?.LogDebug(ex, "Failed to read real-time events for stream {Stream}, falling back to captured events", stream);
                }
            }

            // Fallback to reading from captured events
            var containerClient = _blobServiceClient.GetBlobContainerClient(_captureContainerName);
            var blobPrefix = GetBlobPrefix(stream);
            var blobs = containerClient.GetBlobsAsync(prefix: blobPrefix, cancellationToken: cancellationToken);

            var allEvents = new List<StreamEvent>();
            await foreach (var blobItem in blobs) {
                var blobClient = containerClient.GetBlobClient(blobItem.Name);
                var streamEvents = await ReadEventsFromBlob(blobClient, stream, cancellationToken).NoContext();
                allEvents.AddRange(streamEvents);
            }

            if (allEvents.Count > 0) {
                // Version is 0-indexed: 1 event → version 0
                return allEvents.Count - 1;
            }

            return null;
        } catch (Exception ex) {
            _logger?.LogWarning(ex, "Failed to get current stream version for {Stream}", stream);
            return null;
        }
    }

    public void Dispose() {
        if (_disposed) return;

        _consumer.Dispose();
        _producerClient.DisposeAsync().AsTask().Wait();
        _disposed = true;
    }
}
