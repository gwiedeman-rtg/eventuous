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
using Eventuous.Azure.EventHubs.Versioning;
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
    readonly IStreamVersionStrategy             _versionStrategy;

    bool _disposed;

    /// <summary>
    /// Ensure the capture container exists, create it if it doesn't
    /// </summary>
    async Task EnsureCaptureContainerExists(CancellationToken cancellationToken) {
        try {
            var containerClient = _blobServiceClient.GetBlobContainerClient(_captureContainerName);
            var response = await containerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).NoContext();
            if (response != null) {
                _logger?.LogDebug("Created capture container {ContainerName}", _captureContainerName);
            } else {
                _logger?.LogDebug("Capture container {ContainerName} already exists", _captureContainerName);
            }
        } catch (RequestFailedException ex) when (ex.Status == 409) {
            // Container was created by another thread/process concurrently, which is fine
            _logger?.LogDebug("Container {ContainerName} already exists (concurrent creation)", _captureContainerName);
        } catch (Exception ex) {
            _logger?.LogWarning(ex, "Failed to ensure capture container {ContainerName} exists", _captureContainerName);
            // Continue anyway - the actual operation will fail if container doesn't exist
            // This allows the operation to provide a more specific error message
        }
    }

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
    /// <param name="versionStrategy">Version strategy instance. If not provided, will be created based on other parameters.</param>
    /// <param name="versionStrategyFactory">Optional factory for creating version strategy. Used if versionStrategy is null.</param>
    /// <param name="tableServiceClient">Optional table service client for atomic versioning</param>
    /// <param name="enableAtomicVersioning">Whether to enable atomic version control (used if versionStrategy is null)</param>
    /// <param name="versionLockContainer">Container name for blob lease versioning (used if versionStrategy is null)</param>
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
            ILoggerFactory?                     loggerFactory = null,
            IStreamVersionStrategy?             versionStrategy = null,
            IVersionStrategyFactory?            versionStrategyFactory = null,
            TableServiceClient?                 tableServiceClient = null,
            bool                                enableAtomicVersioning = false,
            string?                             versionLockContainer = null
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

        // Initialize version strategy - prefer injected strategy, then factory, then create based on configuration
        // Note: For NonAtomicVersionStrategy, we need to pass 'this', so we do it after all fields are initialized
        if (versionStrategy != null) {
            _versionStrategy = versionStrategy;
        } else if (versionStrategyFactory != null) {
            _versionStrategy = versionStrategyFactory.CreateVersionStrategy(new VersionStrategyContext {
                EventStore = this,
                TableServiceClient = tableServiceClient,
                BlobServiceClient = blobServiceClient,
                VersionLockContainerName = versionLockContainer,
                EnableAtomicVersioning = enableAtomicVersioning,
                LoggerFactory = loggerFactory
            });
        } else if (enableAtomicVersioning) {
            _versionStrategy = tableServiceClient != null
                ? new TableStorageVersionStrategy(tableServiceClient, loggerFactory?.CreateLogger<TableStorageVersionStrategy>())
                : new BlobLeaseVersionStrategy(blobServiceClient, versionLockContainer ?? "eventuous-locks", loggerFactory?.CreateLogger<BlobLeaseVersionStrategy>());
        } else {
            // NonAtomicVersionStrategy requires 'this' reference, which is now fully initialized
            _versionStrategy = new NonAtomicVersionStrategy(this, loggerFactory?.CreateLogger<NonAtomicVersionStrategy>());
        }
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
    /// <param name="loggerFactory">Optional logger factory</param>
    /// <param name="versionStrategy">Optional version strategy instance</param>
    /// <param name="versionStrategyFactory">Optional factory for creating version strategy</param>
    /// <param name="tableStorageConnectionString">Optional table storage connection string for atomic versioning</param>
    /// <param name="enableAtomicVersioning">Whether to enable atomic version control (used if versionStrategy is null)</param>
    /// <param name="versionLockContainer">Container name for blob lease versioning (used if versionStrategy is null)</param>
    public AzureEventHubsEventStore(
            string                              eventHubConnectionString,
            string                              eventHubName,
            string                              blobStorageConnectionString,
            string                              captureContainerName,
            string                              consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName,
            bool                                useRealtimeReading = true,
            IEventSerializer?                   serializer     = null,
            IMetadataSerializer?                metaSerializer = null,
            ILogger<AzureEventHubsEventStore>?  logger         = null,
            ILoggerFactory?                     loggerFactory = null,
            IStreamVersionStrategy?             versionStrategy = null,
            IVersionStrategyFactory?            versionStrategyFactory = null,
            string?                             tableStorageConnectionString = null,
            bool                                enableAtomicVersioning = false,
            string?                             versionLockContainer = null
        ) : this(
            new EventHubProducerClient(Ensure.NotEmptyString(eventHubConnectionString), Ensure.NotEmptyString(eventHubName)),
            new EventHubConsumerClient(consumerGroup, Ensure.NotEmptyString(eventHubConnectionString), Ensure.NotEmptyString(eventHubName)),
            new BlobServiceClient(Ensure.NotEmptyString(blobStorageConnectionString)),
            eventHubName,
            captureContainerName,
            useRealtimeReading,
            serializer,
            metaSerializer,
            logger,
            loggerFactory,
            versionStrategy,
            versionStrategyFactory,
            !string.IsNullOrWhiteSpace(tableStorageConnectionString) ? new TableServiceClient(tableStorageConnectionString, new TableClientOptions()) : null,
            enableAtomicVersioning,
            versionLockContainer
        ) { }

    /// <inheritdoc/>
    public async Task<bool> StreamExists(StreamName stream, CancellationToken cancellationToken = default) {
        try {
            // Try to read from Event Hubs with a shorter timeout first
            // Events are immediately available in Event Hubs even before Capture writes them to blob storage
            if (_useRealtimeReading) {
                try {
                    var streamEvents = await _consumer.ReadEventsFromStream(
                        stream,
                        EventPosition.Earliest,
                        1, // Just check if any events exist
                        TimeSpan.FromSeconds(30), // Increased timeout to allow events to propagate
                        cancellationToken
                    ).NoContext();

                    if (streamEvents.Length > 0) {
                        return true;
                    }
                } catch (OperationCanceledException) {
                    // Timeout is expected, just fall through to blob check
                    _logger?.LogDebug("Read timeout for stream {Stream}, checking blobs instead", stream);
                } catch (Exception ex) {
                    _logger?.LogDebug(ex, "Failed to read from Event Hubs for stream {Stream}, checking blobs", stream);
                }
            }

            // Always check blob storage for captured events as fallback
            // Note: Azure Event Hubs Capture writes events to blob storage asynchronously,
            // so newly written events might not be immediately available here
            await EnsureCaptureContainerExists(cancellationToken).NoContext();
            var containerClient = _blobServiceClient.GetBlobContainerClient(_captureContainerName);
            var blobPages = containerClient.GetBlobsAsync(prefix: GetBlobPrefix(stream), cancellationToken: cancellationToken)
                .AsPages(pageSizeHint: 10); // Check more pages since we need to parse events from blobs

            var foundEvents = new List<StreamEvent>();
            await foreach (var page in blobPages) {
                foreach (var blobItem in page.Values) {
                    var blobClient = containerClient.GetBlobClient(blobItem.Name);
                    try {
                        var streamEventsFromBlob = await ReadEventsFromBlob(blobClient, stream, cancellationToken).NoContext();
                        if (streamEventsFromBlob.Any()) {
                            return true; // Found events for this stream
                        }
                    } catch {
                        // Ignore individual blob read errors
                    }
                }

                // Limit how many blobs we check
                if (foundEvents.Count > 0) break;
                break; // Only check first page to avoid long operations
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
            long? currentVersion = null;
            var isAtomicStrategy = _versionStrategy is TableStorageVersionStrategy || _versionStrategy is BlobLeaseVersionStrategy;

            // Use version strategy to get current version and validate
            // For atomic strategies, we MUST get the version before proceeding - no skipping validation
            try {
                currentVersion = await _versionStrategy.GetVersion(stream, cancellationToken).NoContext();
            } catch (Exception ex) {
                if (isAtomicStrategy) {
                    // For atomic strategies, we cannot skip validation - fail if we can't get version
                    _logger?.LogError(ex, "Failed to get version for stream {Stream} - cannot proceed with atomic versioning strategy", stream);
                    throw new AppendToStreamException(stream, new InvalidOperationException("Unable to retrieve stream version for validation", ex));
                } else {
                    // For non-atomic strategies, we can continue (but validation will be unreliable)
                    _logger?.LogWarning(ex, "Failed to get version for stream {Stream}, continuing with append (non-atomic strategy)", stream);
                    currentVersion = null;
                }
            }

            // Handle NoStream case - use version strategy to check if stream exists
            if (expectedVersion == ExpectedStreamVersion.NoStream) {
                // For atomic versioning strategies (TableStorage, BlobLease), use GetVersion to check if stream exists
                // For non-atomic strategies, fall back to StreamExists
                if (isAtomicStrategy) {
                    // We already got the version above, check if it exists
                    if (currentVersion.HasValue && currentVersion.Value >= 0) {
                        throw new AppendToStreamException(stream, new InvalidOperationException($"WrongExpectedVersion {-1}, stream already exists"));
                    }
                } else {
                    // For NonAtomicVersionStrategy, use the unreliable StreamExists method
                    var streamExists = await StreamExists(stream, cancellationToken).NoContext();
                    if (streamExists) {
                        throw new AppendToStreamException(stream, new InvalidOperationException($"WrongExpectedVersion {-1}, stream already exists"));
                    }
                }
            }
            // Check expected version for optimistic concurrency (skip for Any)
            else if (expectedVersion != ExpectedStreamVersion.Any) {
                // Expected version is a specific number (0, 1, 2, ...)
                if (isAtomicStrategy) {
                    // For atomic strategies, we MUST have a version to validate - this prevents race conditions
                    if (!currentVersion.HasValue) {
                        // Should not happen if GetVersion succeeded above, but handle it defensively
                        throw new AppendToStreamException(stream, new InvalidOperationException("Stream version is required for validation but not available"));
                    }

                    if (currentVersion.Value != expectedVersion.Value) {
                        throw new AppendToStreamException(stream, new InvalidOperationException($"WrongExpectedVersion {expectedVersion.Value}, current version {currentVersion.Value}"));
                    }
                } else {
                    // For non-atomic strategies, validation is best-effort
                    if (!currentVersion.HasValue) {
                        _logger?.LogDebug("Could not get version for stream {Stream}, skipping version validation (non-atomic strategy)", stream);
                    } else if (currentVersion.Value != expectedVersion.Value) {
                        throw new AppendToStreamException(stream, new InvalidOperationException($"WrongExpectedVersion {expectedVersion.Value}, current version {currentVersion.Value}"));
                    }
                }
            }

            // Pin events for a given stream to a specific partition for deterministic real-time reads
            var batchOptions   = new CreateBatchOptions { PartitionKey = stream.ToString() };
            var eventDataBatch = await _producerClient.CreateBatchAsync(batchOptions, cancellationToken).NoContext();
            var eventPosition = 0L;

            foreach (var streamEvent in events) {
                var eventData = ToEventData(streamEvent, stream);

                if (!eventDataBatch.TryAdd(eventData)) {
                    // If the batch is full, send it and create a new batch
                    await _producerClient.SendAsync(eventDataBatch, cancellationToken).NoContext();
                    eventDataBatch = await _producerClient.CreateBatchAsync(batchOptions, cancellationToken).NoContext();

                    if (!eventDataBatch.TryAdd(eventData)) {
                        throw new InvalidOperationException("Event is too large to fit in a batch");
                    }
                }
                eventPosition++;
            }

            // Send the final batch
            if (eventDataBatch.Count > 0) {
                _logger?.LogInformation("Sending {Count} events to Event Hubs for stream {Stream}", eventDataBatch.Count, stream);
                await _producerClient.SendAsync(eventDataBatch, cancellationToken).NoContext();
                _logger?.LogInformation("Successfully sent {Count} events to Event Hubs for stream {Stream}", eventDataBatch.Count, stream);

                // Give Event Hubs a moment to commit the events before they become readable
                // Increase a bit for emulator stability
                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken).NoContext();
            }

            // Calculate next version atomically
            // Note: We've already validated the version above, but IncrementVersion will perform a final atomic check
            // to ensure no concurrent modifications occurred between validation and now. This provides strong consistency.
            long nextExpectedVersion;
            try {
                var expectedVersionValue = expectedVersion == ExpectedStreamVersion.NoStream ? -1 : expectedVersion.Value;
                nextExpectedVersion = await _versionStrategy.IncrementVersion(stream, expectedVersionValue, events.Count, cancellationToken).NoContext();
            } catch (AppendToStreamException) {
                // Re-throw version validation exceptions - these indicate concurrent modifications or race conditions
                // that occurred between our initial validation and the atomic increment
                throw;
            } catch (Exception ex) {
                // For infrastructure errors (network, storage unavailable, etc.), fall back to local calculation
                // This should be rare and only for non-atomic strategies or when infrastructure is unreliable
                _logger?.LogWarning(ex, "Failed to increment version for stream {Stream}, calculating locally", stream);
                nextExpectedVersion = currentVersion.HasValue ? currentVersion.Value + events.Count : events.Count - 1;
            }

            // Event Hubs doesn't provide a global position like EventStore, so we use a timestamp-based approach
            var globalPosition = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

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
            var allRealtimeEvents = new List<StreamEvent>();

            // First, try to read from real-time Event Hubs if enabled (events are immediately available)
            // We'll retry a few times to account for propagation delays
            // For recent events, try Latest first (more efficient), then fall back to Earliest if needed
            if (_useRealtimeReading) {
                const int maxRetries = 5;
                for (int attempt = 0; attempt < maxRetries; attempt++) {
                    try {
                        // For reliability with emulator, start from the beginning to ensure we see the write
                        var position = EventPosition.Earliest;
                        var timeout  = TimeSpan.FromSeconds(15); // generous timeout per attempt

                        // Read more events than needed to ensure we get all available events for de-duplication
                        var realtimeEvents = await _consumer.ReadEventsFromStream(
                            stream,
                            position,
                            Math.Max(count * 2, 100), // Read more to account for potential duplicates with blob storage
                            timeout,
                            cancellationToken
                        ).NoContext();

                        if (realtimeEvents.Length > 0) {
                            allRealtimeEvents.AddRange(realtimeEvents);
                            _logger?.LogDebug("Read {Count} real-time events from stream {Stream} (attempt {Attempt})",
                                realtimeEvents.Length, stream, attempt + 1);

                            // Apply start position and count after combining with potential blob events
                            events.AddRange(realtimeEvents.Skip((int)start.Value).Take(count));

                            if (events.Count >= count) {
                                // We have enough from Event Hubs, return immediately
                                return events.Take(count).ToArray();
                            }
                        } else if (attempt < maxRetries - 1) {
                            // No events yet, wait a bit and retry (events might still be propagating)
                            _logger?.LogDebug("No events found in real-time read for stream {Stream} (attempt {Attempt}), waiting before retry",
                                stream, attempt + 1);
                            await Task.Delay(TimeSpan.FromMilliseconds(500 * (attempt + 1)), cancellationToken).NoContext();
                            continue;
                        }
                        break; // Either we have events or we've exhausted retries
                    } catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested) {
                        // Timeout occurred, try again if we have retries left
                        if (attempt < maxRetries - 1) {
                            _logger?.LogDebug("Real-time read timeout for stream {Stream} (attempt {Attempt}), retrying",
                                stream, attempt + 1);
                            await Task.Delay(TimeSpan.FromMilliseconds(500 * (attempt + 1)), cancellationToken).NoContext();
                            continue;
                        }
                        _logger?.LogDebug("Real-time read timeout for stream {Stream} after {Attempts} attempts, will try captured events",
                            stream, maxRetries);
                        break;
                    } catch (Exception ex) {
                        _logger?.LogDebug(ex, "Failed to read real-time events from stream {Stream} (attempt {Attempt}), will try captured events",
                            stream, attempt + 1);
                        break; // Don't retry on non-timeout exceptions
                    }
                }
            }

            // If we don't have enough events from real-time, also read from captured events (blob storage)
            // Note: Event Hubs Capture writes events asynchronously, so recently written events may not be in blob storage yet
            // We'll combine both sources and de-duplicate based on event ID
            var capturedEvents = new List<StreamEvent>();
            try {
                // Ensure container exists before reading (gracefully handles if it doesn't exist)
                await EnsureCaptureContainerExists(cancellationToken).NoContext();
                var fromCapture = await ReadEventsFromCapture(stream, start, count * 2, cancellationToken).NoContext();
                capturedEvents.AddRange(fromCapture);
            } catch (RequestFailedException ex) when (ex.Status == 404 && ex.ErrorCode == "ContainerNotFound") {
                // Container doesn't exist yet - this is fine, just means capture hasn't started or container wasn't created
                _logger?.LogDebug("Capture container {ContainerName} does not exist yet for stream {Stream}, using Event Hubs events only",
                    _captureContainerName, stream);
            } catch (Exception ex) {
                _logger?.LogDebug(ex, "Failed to read captured events from stream {Stream}, using Event Hubs events only", stream);
            }

            // De-duplicate events from both sources (by EventId) and combine
            // Real-time events take priority (they're the source of truth for recent events)
            var eventMap = new Dictionary<string, StreamEvent>();

            // Add captured events first (older events)
            foreach (var evt in capturedEvents) {
                var eventId = evt.Id.ToString();
                if (!eventMap.ContainsKey(eventId)) {
                    eventMap[eventId] = evt;
                }
            }

            // Add real-time events (overwrite captured events if duplicates, since real-time is authoritative)
            foreach (var evt in allRealtimeEvents) {
                var eventId = evt.Id.ToString();
                eventMap[eventId] = evt; // Real-time events take priority
            }

            // Combine all events, sort by position, and apply start position
            var allEvents = eventMap.Values
                .OrderBy(e => e.Position)
                .Skip((int)start.Value)
                .Take(count)
                .ToArray();

            // Only throw StreamNotFound if we have no events AND failIfNotFound is true
            // This handles the case where events were just written and haven't propagated yet
            if (!allEvents.Any()) {
                if (failIfNotFound) {
                    _logger?.LogDebug("No events found for stream {Stream} from either Event Hubs or blob storage", stream);
                    throw new StreamNotFound(stream);
                }
                return [];
            }

            return allEvents;
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

            // First, try to read from real-time Event Hubs (recent events may not be in blob storage yet)
            if (_useRealtimeReading) {
                try {
                    var realtimeEvents = await _consumer.ReadEventsFromStream(
                        stream,
                        EventPosition.Earliest,
                        Math.Max(count * 2, 100),
                        TimeSpan.FromSeconds(30),
                        cancellationToken
                    ).NoContext();
                    allEvents.AddRange(realtimeEvents);
                } catch (Exception ex) {
                    _logger?.LogDebug(ex, "Failed to read real-time events from stream {Stream} for backwards read, using captured events only", stream);
                }
            }

            // Also read from captured events in blob storage
            try {
                await EnsureCaptureContainerExists(cancellationToken).NoContext();
                var containerClient = _blobServiceClient.GetBlobContainerClient(_captureContainerName);
                var blobPrefix = GetBlobPrefix(stream);
                var blobs = containerClient.GetBlobsAsync(prefix: blobPrefix, cancellationToken: cancellationToken);

                // Collect all events from blobs
                await foreach (var blobItem in blobs) {
                    try {
                        var blobClient = containerClient.GetBlobClient(blobItem.Name);
                        var streamEvents = await ReadEventsFromBlob(blobClient, stream, cancellationToken).NoContext();
                        allEvents.AddRange(streamEvents);
                    } catch (Exception ex) {
                        _logger?.LogDebug(ex, "Failed to read events from blob {BlobName} for stream {Stream}", blobItem.Name, stream);
                    }
                }
            } catch (RequestFailedException ex) when (ex.Status == 404 && ex.ErrorCode == "ContainerNotFound") {
                _logger?.LogDebug("Capture container does not exist for backwards read, using Event Hubs events only");
            } catch (Exception ex) {
                _logger?.LogDebug(ex, "Failed to read captured events for backwards read, using Event Hubs events only");
            }

            // De-duplicate by EventId (real-time events take priority)
            var eventMap = new Dictionary<string, StreamEvent>();
            foreach (var evt in allEvents) {
                var eventId = evt.Id.ToString();
                if (!eventMap.ContainsKey(eventId)) {
                    eventMap[eventId] = evt;
                }
            }

            if (!eventMap.Any() && failIfNotFound) {
                throw new StreamNotFound(stream);
            }

            // Sort by position and take from the end
            var sortedEvents = eventMap.Values.OrderBy(e => e.Position).ToArray();
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
        _logger?.LogInformation("ToEventData called for stream {Stream}, EventId={EventId}", stream, streamEvent.Id);

        var (eventType, contentType, payload) = _serializer.SerializeEvent(streamEvent.Payload!);
        _logger?.LogInformation("Serialized event: Type={EventType}, ContentType={ContentType}, PayloadLength={Length}, Stream={Stream}",
            eventType, contentType, payload.Length, stream);

        var metadata = _metaSerializer.Serialize(streamEvent.Metadata);
        _logger?.LogInformation("Serialized metadata: Length={Length}", metadata.Length);

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

        _logger?.LogInformation("Created EventData: MessageId={MessageId}, BodyLength={BodyLength}, Properties={Properties}",
            eventData.MessageId, eventData.EventBody.Length,
            string.Join(", ", eventData.Properties.Select(kvp => $"{kvp.Key}={kvp.Value}")));

        return eventData;
    }

    async Task<StreamEvent[]> ReadEventsFromCapture(
            StreamName        stream,
            StreamReadPosition start,
            int               count,
            CancellationToken cancellationToken
        ) {
        var events = new List<StreamEvent>();

        try {
            // Ensure container exists before reading
            await EnsureCaptureContainerExists(cancellationToken).NoContext();
            var containerClient = _blobServiceClient.GetBlobContainerClient(_captureContainerName);

            // Get captured event blobs for this stream
            var blobPrefix = GetBlobPrefix(stream);
            var blobs = containerClient.GetBlobsAsync(prefix: blobPrefix, cancellationToken: cancellationToken);

            var processedEvents = 0;
            var skippedEvents = 0;

            await foreach (var blobItem in blobs) {
                if (events.Count >= count) break;

                try {
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
                } catch (Exception ex) {
                    _logger?.LogDebug(ex, "Failed to read events from blob {BlobName} for stream {Stream}", blobItem.Name, stream);
                    // Continue reading other blobs
                }
            }
        } catch (RequestFailedException ex) when (ex.Status == 404 && ex.ErrorCode == "ContainerNotFound") {
            // Container doesn't exist yet - return empty list (events may not be captured yet)
            _logger?.LogDebug("Capture container {ContainerName} does not exist for stream {Stream}, returning empty",
                _captureContainerName, stream);
            return [];
        } catch (Exception ex) {
            _logger?.LogDebug(ex, "Failed to read captured events for stream {Stream}, returning partial results", stream);
            // Return what we have so far
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
    /// Gets the current version of a stream using a hybrid approach
    ///
    /// This method attempts to determine the stream version by:
    /// 1. First trying to read from real-time Event Hubs (if enabled) with retries
    /// 2. Falling back to reading from captured blob storage
    /// 3. Returning null if both fail (allowing non-atomic strategies to skip validation)
    ///
    /// For NonAtomicVersionStrategy, this method uses longer timeouts and retries
    /// to account for Event Hubs propagation delays. This is acceptable since non-atomic
    /// strategies trade some performance for eventual consistency.
    /// </summary>
    /// <param name="stream">Stream name to get version for</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Current stream version, or null if cannot be determined</returns>
    internal async Task<long?> GetCurrentStreamVersion(StreamName stream, CancellationToken cancellationToken) {
        try {
            // Try to read from real-time first with retries to account for propagation delays
            // Non-atomic strategies can use longer timeouts since they're not blocking on consistency
            if (_useRealtimeReading) {
                const int maxRetries = 3;
                const int baseTimeoutSeconds = 5; // Longer timeout for non-atomic strategies

                for (int attempt = 0; attempt < maxRetries; attempt++) {
                    try {
                        var timeout = TimeSpan.FromSeconds(baseTimeoutSeconds * (attempt + 1)); // Exponential backoff
                        var realtimeEvents = await _consumer.ReadEventsFromStream(
                            stream,
                            EventPosition.Earliest,
                            1000,
                            timeout,
                            cancellationToken
                        ).NoContext();

                        if (realtimeEvents.Length > 0) {
                            // Version is 0-indexed: 1 event → version 0
                            _logger?.LogDebug("Got version {Version} from real-time Event Hubs for stream {Stream} (attempt {Attempt})",
                                realtimeEvents.Length - 1, stream, attempt + 1);
                            return realtimeEvents.Length - 1;
                        }

                        // If we got 0 events but no exception, wait a bit and retry (event might still be propagating)
                        if (attempt < maxRetries - 1) {
                            await Task.Delay(TimeSpan.FromMilliseconds(500 * (attempt + 1)), cancellationToken).NoContext();
                            continue;
                        }
                    } catch (OperationCanceledException) {
                        if (attempt < maxRetries - 1) {
                            _logger?.LogDebug("Real-time reading timed out for stream {Stream} (attempt {Attempt}), retrying...", stream, attempt + 1);
                            await Task.Delay(TimeSpan.FromMilliseconds(500 * (attempt + 1)), cancellationToken).NoContext();
                            continue;
                        }
                        _logger?.LogDebug("Real-time reading timed out for stream {Stream} after {Attempts} attempts, falling back to captured events", stream, maxRetries);
                        break;
                    } catch (Exception ex) {
                        _logger?.LogDebug(ex, "Failed to read real-time events for stream {Stream} (attempt {Attempt}), falling back to captured events", stream, attempt + 1);
                        break; // Don't retry on non-timeout exceptions
                    }
                }
            }

            // Fallback to reading from captured events
            await EnsureCaptureContainerExists(cancellationToken).NoContext();
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
                _logger?.LogDebug("Got version {Version} from captured blob storage for stream {Stream}",
                    allEvents.Count - 1, stream);
                return allEvents.Count - 1;
            }

            // If we can't determine the version, return null
            // This allows NonAtomicVersionStrategy to skip version validation
            _logger?.LogDebug("Could not determine version for stream {Stream}, version validation will be skipped", stream);
            return null;
        } catch (Exception ex) {
            _logger?.LogWarning(ex, "Failed to get current stream version for {Stream}, version validation will be skipped", stream);
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
