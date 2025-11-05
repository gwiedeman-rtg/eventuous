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
        var readTimeout = timeout ?? TimeSpan.FromSeconds(10);

        try {
            using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                _cancellationTokenSource.Token
            );

            combinedCts.CancelAfter(readTimeout);

            // Increase MaximumWaitTime to give events more time to become available
            // This helps when reading immediately after writes
            var readOptions = new ReadEventOptions {
                MaximumWaitTime = TimeSpan.FromSeconds(5) // Increased from 1 second to allow more time for events to propagate
            };

            // Read from all partitions to find events for this stream
            var partitionIds = await _consumerClient.GetPartitionIdsAsync().NoContext();
            _logger?.LogDebug("Reading events from {PartitionCount} partitions for stream {Stream}", partitionIds.Length, stream);

            // For Event Hubs, we need to read from all partitions and filter by StreamName property
            // Events might be distributed across partitions, so we read from each partition
            var readTasks = partitionIds.Select(async partitionId => {
                var partitionEvents = new List<StreamEvent>();
                var eventsRead = 0;
                var lastSequenceNumber = -1L;
                var nonMatchingSeen = 0;

                try {
                    await foreach (var partitionEvent in _consumerClient.ReadEventsFromPartitionAsync(
                        partitionId,
                        startPosition,
                        readOptions,
                        combinedCts.Token
                    )) {
                        eventsRead++;

                        // Track progress - if we're seeing events but none match our stream, log it
                        if (partitionEvent.Data != null) {
                            lastSequenceNumber = partitionEvent.Data.SequenceNumber;
                        }

                        // Break early if we have enough matching events
                        if (partitionEvents.Count >= maxEvents) {
                            _logger?.LogDebug("Found enough events ({Count}) from partition {PartitionId} for stream {Stream}",
                                partitionEvents.Count, partitionId, stream);
                            break;
                        }

                        // Skip events with null Data (system events from emulator)
                        if (partitionEvent.Data == null) {
                            _logger?.LogTrace("Skipping event with null Data from partition {PartitionId}", partitionId);
                            continue;
                        }

                        // Filter by StreamName property
                        var streamEvent = ConvertToStreamEvent(partitionEvent, stream);
                        if (streamEvent != null) {
                            partitionEvents.Add(streamEvent.Value);
                            _logger?.LogTrace("Found matching event for stream {Stream} from partition {PartitionId}, SequenceNumber={SequenceNumber}",
                                stream, partitionId, partitionEvent.Data.SequenceNumber);
                        } else {
                            nonMatchingSeen++;
                        }

                        // Stop reading from this partition if we've read many events but none match
                        // This prevents infinite loops on partitions that don't have our stream's events
                        if (eventsRead > maxEvents * 10 && partitionEvents.Count == 0) {
                            _logger?.LogDebug("Stopped reading from partition {PartitionId} after {EventsRead} events with no matches for stream {Stream}, last SequenceNumber={SequenceNumber}",
                                partitionId, eventsRead, stream, lastSequenceNumber);
                            break;
                        }
                    }
                } catch (OperationCanceledException) when (!combinedCts.Token.IsCancellationRequested) {
                    // Timeout - this is expected when reading from a partition with no new events
                    _logger?.LogTrace("Read timeout from partition {PartitionId} for stream {Stream} after reading {EventsRead} events, found {MatchingCount} matches",
                        partitionId, stream, eventsRead, partitionEvents.Count);
                }

                if (partitionEvents.Count > 0) {
                    _logger?.LogDebug("Found {Count} matching events from partition {PartitionId} for stream {Stream}",
                        partitionEvents.Count, partitionId, stream);
                } else {
                    _logger?.LogDebug("No matching events from partition {PartitionId} for stream {Stream}. Read {EventsRead} events, {NonMatching} did not match.",
                        partitionId, stream, eventsRead, nonMatchingSeen);
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

            if (events.Count == 0) {
                _logger?.LogDebug("ReadEventsFromStream found 0 matching events for {Stream} when reading per-partition. Falling back to ReadEventsAsync.", stream);

                var fallback = new List<StreamEvent>();
                var eventsRead = 0;

                try {
                    await foreach (var ev in _consumerClient.ReadEventsAsync(readOptions, combinedCts.Token)) {
                        eventsRead++;

                        if (ev.Data == null) continue;

                        var streamEvent = ConvertToStreamEvent(ev, stream);
                        if (streamEvent != null) {
                            fallback.Add(streamEvent.Value);
                            if (fallback.Count >= maxEvents) break;
                        }
                    }
                } catch (OperationCanceledException) when (!combinedCts.Token.IsCancellationRequested) {
                    _logger?.LogTrace("Fallback ReadEventsAsync timeout for {Stream} after reading {EventsRead} events, found {Matching}", stream, eventsRead, fallback.Count);
                }

                if (fallback.Count > 0) {
                    events = fallback.OrderBy(x => x.Position).Take(maxEvents).ToList();
                }
            }

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

                    // Skip events with null Data (system events from emulator)
                    if (partitionEvent.Data == null) {
                        _logger?.LogTrace("Skipping event with null Data from partition {PartitionId}", partitionId);
                        continue;
                    }

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
                var expectedStreamName = targetStream.ToString();

                // Try common property casings/keys
                string? actualStreamName = null;
                if (eventData.Properties.TryGetValue("StreamName", out var s1)) actualStreamName = s1?.ToString();
                else if (eventData.Properties.TryGetValue("streamName", out var s2)) actualStreamName = s2?.ToString();
                else if (eventData.Properties.TryGetValue("stream", out var s3)) actualStreamName = s3?.ToString();

                if (actualStreamName != expectedStreamName) {
                    _logger?.LogTrace("Event belongs to different stream: Expected={Expected}, Actual={Actual}, MessageId={MessageId}",
                        expectedStreamName, actualStreamName ?? "null", eventData.MessageId);
                    return null;
                }

                _logger?.LogTrace("Event matches target stream: Stream={Stream}, MessageId={MessageId}", targetStream, eventData.MessageId);
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