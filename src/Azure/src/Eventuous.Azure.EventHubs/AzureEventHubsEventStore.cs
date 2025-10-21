// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using Eventuous.Tools;
using static Eventuous.DeserializationResult;
using static Eventuous.Diagnostics.PersistenceEventSource;

namespace Eventuous.Azure.EventHubs;

/// <summary>
/// Azure Event Hubs implementation of <see cref="IEventStore"/> with Capture support
/// </summary>
public class AzureEventHubsEventStore : IEventStore, IDisposable {
    readonly ILogger<AzureEventHubsEventStore>? _logger;
    readonly EventHubProducerClient             _producerClient;
    readonly BlobServiceClient                  _blobServiceClient;
    readonly TableServiceClient                 _tableServiceClient;
    readonly AzureEventHubsConsumer             _consumer;
    readonly IEventSerializer                   _serializer;
    readonly IMetadataSerializer                _metaSerializer;
    readonly string                             _eventHubName;
    readonly string                             _captureContainerName;
    readonly bool                               _useRealtimeReading;

    bool _disposed;

    /// <summary>
    /// Initialize the event store with Event Hub producer and Blob storage client for reading captured events
    /// </summary>
    /// <param name="producerClient">Event Hub producer client instance</param>
    /// <param name="consumerClient">Event Hub consumer client for real-time reading</param>
    /// <param name="blobServiceClient">Blob service client for reading captured events</param>
    /// <param name="tableServiceClient">Table service client for stream metadata and versioning</param>
    /// <param name="eventHubName">Event Hub name</param>
    /// <param name="captureContainerName">Blob container name where captured events are stored</param>
    /// <param name="useRealtimeReading">Whether to use real-time reading from Event Hubs for recent events</param>
    /// <param name="serializer">Optional event serializer. When not provided, the default serializer will be used.</param>
    /// <param name="metaSerializer">Optional metadata serializer. When not provided, the default serializer will be used.</param>
    /// <param name="logger">Optional logger</param>
    public AzureEventHubsEventStore(
            EventHubProducerClient              producerClient,
            EventHubConsumerClient              consumerClient,
            BlobServiceClient                   blobServiceClient,
            TableServiceClient                  tableServiceClient,
            string                              eventHubName,
            string                              captureContainerName,
            bool                                useRealtimeReading = true,
            IEventSerializer?                   serializer     = null,
            IMetadataSerializer?                metaSerializer = null,
            ILogger<AzureEventHubsEventStore>?  logger         = null
        ) {
        _logger               = logger;
        _producerClient       = Ensure.NotNull(producerClient);
        _blobServiceClient    = Ensure.NotNull(blobServiceClient);
        _tableServiceClient   = Ensure.NotNull(tableServiceClient);
        _eventHubName         = Ensure.NotEmptyString(eventHubName);
        _captureContainerName = Ensure.NotEmptyString(captureContainerName);
        _useRealtimeReading   = useRealtimeReading;
        _serializer           = serializer     ?? DefaultEventSerializer.Instance;
        _metaSerializer       = metaSerializer ?? DefaultMetadataSerializer.Instance;
        _consumer             = new AzureEventHubsConsumer(consumerClient, serializer, metaSerializer, logger);
    }

    /// <summary>
    /// Initialize the event store with connection strings
    /// </summary>
    /// <param name="eventHubConnectionString">Event Hub connection string</param>
    /// <param name="eventHubName">Event Hub name</param>
    /// <param name="blobStorageConnectionString">Blob storage connection string</param>
    /// <param name="tableStorageConnectionString">Table storage connection string</param>
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
            string                              tableStorageConnectionString,
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
            new TableServiceClient(Ensure.NotEmptyString(tableStorageConnectionString)),
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
            // Check if stream exists in Table Storage metadata
            const string tableName = "StreamMetadata";
            const string partitionKey = "streams";
            var rowKey = stream.ToString();

            var tableClient = _tableServiceClient.GetTableClient(tableName);

            try {
                var response = await tableClient.GetEntityAsync<TableEntity>(partitionKey, rowKey, cancellationToken: cancellationToken).NoContext();
                var version = response.Value.GetInt32("Version");
                return version >= 0; // Version >= 0 means stream exists
            } catch (RequestFailedException ex) when (ex.Status == 404) {
                return false; // Stream doesn't exist
            }
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
            // CRITICAL: Validate and update stream version with optimistic concurrency
            var (newVersion, globalPosition) = await ValidateAndUpdateVersion(stream, expectedVersion, events.Count, cancellationToken).NoContext();

            // Create batch with partition key for stream isolation
            var batchOptions = new CreateBatchOptions { PartitionKey = stream.ToString() };
            var eventDataBatch = await _producerClient.CreateBatchAsync(batchOptions, cancellationToken).NoContext();
            var streamPosition = newVersion - events.Count; // Starting position for this batch

            foreach (var streamEvent in events) {
                var eventData = ToEventData(streamEvent, stream, streamPosition);

                if (!eventDataBatch.TryAdd(eventData)) {
                    // If the batch is full, send it and create a new batch
                    await _producerClient.SendAsync(eventDataBatch, cancellationToken).NoContext();
                    eventDataBatch = await _producerClient.CreateBatchAsync(batchOptions, cancellationToken).NoContext();

                    if (!eventDataBatch.TryAdd(eventData)) {
                        throw new InvalidOperationException("Event is too large to fit in a batch");
                    }
                }
                streamPosition++;
            }

            // Send the final batch
            if (eventDataBatch.Count > 0) {
                await _producerClient.SendAsync(eventDataBatch, cancellationToken).NoContext();
            }

            return new AppendEventsResult(globalPosition, newVersion);
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

    /// <summary>
    /// Validates expected version and atomically updates stream version using Table Storage
    /// </summary>
    async Task<(long newVersion, ulong globalPosition)> ValidateAndUpdateVersion(
            StreamName stream,
            ExpectedStreamVersion expectedVersion,
            int eventCount,
            CancellationToken cancellationToken
        ) {
        const string tableName = "StreamMetadata";
        const string partitionKey = "streams";
        var rowKey = stream.ToString();

        try {
            // Get current stream metadata
            var tableClient = _tableServiceClient.GetTableClient(tableName);
            await tableClient.CreateIfNotExistsAsync(cancellationToken).NoContext();

            TableEntity? currentEntity = null;
            try {
                var response = await tableClient.GetEntityAsync<TableEntity>(partitionKey, rowKey, cancellationToken: cancellationToken).NoContext();
                currentEntity = response.Value;
            } catch (RequestFailedException ex) when (ex.Status == 404) {
                // Stream doesn't exist yet
            }

            var currentVersion = currentEntity?.GetInt32("Version") ?? -1; // -1 means NoStream
            var currentGlobalPosition = currentEntity?.GetInt64("GlobalPosition") ?? 0L;

            // Validate expected version
            if (expectedVersion == ExpectedStreamVersion.NoStream) {
                if (currentVersion != -1) {
                    throw new AppendToStreamException(stream, new InvalidOperationException($"Stream {stream} already exists"));
                }
            } else if (expectedVersion == ExpectedStreamVersion.Any) {
                // Any version is acceptable
            } else {
                if (currentVersion != expectedVersion.Value) {
                    throw new AppendToStreamException(stream, new InvalidOperationException($"Wrong expected version. Expected: {expectedVersion.Value}, Current: {currentVersion}"));
                }
            }

            // Calculate new version and global position
            var newVersion = currentVersion + eventCount;
            var newGlobalPosition = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Update or create stream metadata atomically
            var newEntity = new TableEntity(partitionKey, rowKey) {
                ["Version"] = newVersion,
                ["GlobalPosition"] = newGlobalPosition,
                ["LastUpdated"] = DateTimeOffset.UtcNow
            };

            if (currentEntity != null) {
                // Update existing stream with ETag for optimistic concurrency
                newEntity.ETag = currentEntity.ETag;
                await tableClient.UpdateEntityAsync(newEntity, newEntity.ETag, cancellationToken: cancellationToken).NoContext();
            } else {
                // Create new stream
                await tableClient.AddEntityAsync(newEntity, cancellationToken: cancellationToken).NoContext();
            }

            return (newVersion, newGlobalPosition);
        } catch (RequestFailedException ex) when (ex.Status == 412) {
            // ETag mismatch - concurrent update occurred
            throw new AppendToStreamException(stream, new InvalidOperationException($"Concurrent update detected for stream {stream}"));
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to validate and update version for stream {Stream}", stream);
            throw new AppendToStreamException(stream, ex);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    EventData ToEventData(NewStreamEvent streamEvent, StreamName stream, long streamPosition) {
        var (eventType, contentType, payload) = _serializer.SerializeEvent(streamEvent.Payload!);
        var metadata = _metaSerializer.Serialize(streamEvent.Metadata);

        var eventData = new EventData(payload) {
            MessageId = streamEvent.Id.ToString(),
            ContentType = contentType,
            PartitionKey = stream.ToString() // Use stream name as partition key for stream isolation
        };

        // Add custom properties for event type, metadata, and stream position
        eventData.Properties["EventType"] = eventType;
        eventData.Properties["StreamName"] = stream.ToString();
        eventData.Properties["StreamPosition"] = streamPosition.ToString();

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
            var avroData = response.Value.Content.ToArray();

            // Parse AVRO format used by Event Hubs Capture
            using var streamReader = new MemoryStream(avroData);
            using var dataFileReader = DataFileReader<GenericRecord>.OpenReader(streamReader);

            foreach (var record in dataFileReader.NextEntries) {
                try {
                    var eventData = ParseCapturedEvent(record, stream);
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

    StreamEvent? ParseCapturedEvent(GenericRecord record, StreamName targetStream) {
        try {
            // Extract properties from the AVRO record
            if (!record.TryGetValue("Properties", out var propertiesObj) || propertiesObj is not GenericRecord properties) {
                return null;
            }

            if (!properties.TryGetValue("StreamName", out var streamNameObj) || streamNameObj is not string streamName) {
                return null;
            }

            if (streamName != targetStream.ToString()) return null;

            if (!properties.TryGetValue("EventType", out var eventTypeObj) || eventTypeObj is not string eventType) {
                return null;
            }

            if (!record.TryGetValue("Body", out var bodyObj) || bodyObj is not byte[] bodyBytes) {
                return null;
            }

            if (!record.TryGetValue("ContentType", out var contentTypeObj) || contentTypeObj is not string contentType) {
                return null;
            }

            // Deserialize the event
            var deserialized = _serializer.DeserializeEvent(bodyBytes, eventType, contentType);

            if (deserialized is not SuccessfullyDeserialized success) return null;

            // Extract metadata
            Metadata? metadata = null;
            if (properties.TryGetValue("Metadata", out var metadataObj) && metadataObj is byte[] metadataBytes) {
                metadata = _metaSerializer.Deserialize(metadataBytes);
            }

            // Extract stream position
            long streamPosition = 0;
            if (properties.TryGetValue("StreamPosition", out var positionObj) && positionObj is string positionStr) {
                long.TryParse(positionStr, out streamPosition);
            }

            // Extract event ID
            var eventId = Guid.NewGuid(); // Fallback
            if (record.TryGetValue("MessageId", out var messageIdObj) && messageIdObj is string messageId) {
                Guid.TryParse(messageId, out eventId);
            }

            return new StreamEvent(
                eventId,
                success.Payload,
                metadata ?? new Metadata(),
                contentType,
                streamPosition
            );
        } catch (Exception ex) {
            _logger?.LogWarning(ex, "Failed to parse captured event from AVRO record");
            return null;
        }
    }

    public void Dispose() {
        if (_disposed) return;

        _consumer.Dispose();
        _producerClient.Dispose();
        _disposed = true;
    }
}
