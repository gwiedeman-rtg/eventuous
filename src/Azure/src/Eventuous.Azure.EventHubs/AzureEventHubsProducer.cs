// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Runtime.CompilerServices;
using Eventuous.Producers;
using Eventuous.Tools;

namespace Eventuous.Azure.EventHubs;

/// <summary>
/// Azure Event Hubs producer implementation
/// </summary>
public class AzureEventHubsProducer : IProducer<AzureEventHubsProduceOptions>, IAsyncDisposable {
    readonly EventHubProducerClient           _producerClient;
    readonly IEventSerializer                 _serializer;
    readonly IMetadataSerializer              _metaSerializer;
    readonly ILogger<AzureEventHubsProducer>? _logger;

    /// <summary>
    /// Initialize the producer with the given Event Hub producer client
    /// </summary>
    /// <param name="producerClient">Event Hub producer client instance</param>
    /// <param name="serializer">Optional event serializer. When not provided, the default serializer will be used.</param>
    /// <param name="metaSerializer">Optional metadata serializer. When not provided, the default serializer will be used.</param>
    /// <param name="logger">Optional logger</param>
    public AzureEventHubsProducer(
            EventHubProducerClient           producerClient,
            IEventSerializer?                serializer     = null,
            IMetadataSerializer?             metaSerializer = null,
            ILogger<AzureEventHubsProducer>? logger         = null
        ) {
        _producerClient = Ensure.NotNull(producerClient);
        _serializer     = serializer     ?? DefaultEventSerializer.Instance;
        _metaSerializer = metaSerializer ?? DefaultMetadataSerializer.Instance;
        _logger         = logger;
    }

    /// <summary>
    /// Initialize the producer with connection string
    /// </summary>
    /// <param name="connectionString">Event Hub connection string</param>
    /// <param name="eventHubName">Event Hub name</param>
    /// <param name="serializer">Optional event serializer. When not provided, the default serializer will be used.</param>
    /// <param name="metaSerializer">Optional metadata serializer. When not provided, the default serializer will be used.</param>
    /// <param name="logger">Optional logger</param>
    public AzureEventHubsProducer(
            string                           connectionString,
            string                           eventHubName,
            IEventSerializer?                serializer     = null,
            IMetadataSerializer?             metaSerializer = null,
            ILogger<AzureEventHubsProducer>? logger         = null
        ) : this(
            new EventHubProducerClient(Ensure.NotEmptyString(connectionString), Ensure.NotEmptyString(eventHubName)),
            serializer,
            metaSerializer,
            logger
        ) { }

    /// <inheritdoc/>
    public Task Produce(StreamName stream, IEnumerable<ProducedMessage> messages, CancellationToken cancellationToken = default)
        => Produce(stream, messages, null, cancellationToken);

    /// <inheritdoc/>
    public async Task Produce(
            StreamName                     stream,
            IEnumerable<ProducedMessage>   messages,
            AzureEventHubsProduceOptions?  options,
            CancellationToken              cancellationToken = default
        ) {
        cancellationToken.ThrowIfCancellationRequested();

        var messageList = messages.ToList();
        if (!messageList.Any()) return;

        var partitionKey = options?.PartitionKey ?? stream.ToString();
        var batchCount = 0;

        try {
            // Pre-validate that all messages can fit in batches
            await ValidateMessageSizes(messageList, stream, options, partitionKey, cancellationToken).NoContext();

            var eventDataBatch = await _producerClient.CreateBatchAsync(
                new CreateBatchOptions { PartitionKey = partitionKey },
                cancellationToken
            ).NoContext();

            foreach (var message in messageList) {
                cancellationToken.ThrowIfCancellationRequested();

                var eventData = ToEventData(message, stream, options);

                if (!eventDataBatch.TryAdd(eventData)) {
                    // If the batch is full, send it and create a new batch
                    await _producerClient.SendAsync(eventDataBatch, cancellationToken).NoContext();
                    batchCount++;

                    eventDataBatch = await _producerClient.CreateBatchAsync(
                        new CreateBatchOptions { PartitionKey = partitionKey },
                        cancellationToken
                    ).NoContext();

                    if (!eventDataBatch.TryAdd(eventData)) {
                        // This should not happen after pre-validation, but handle gracefully
                        throw new InvalidOperationException($"Event is too large to fit in a batch after validation");
                    }
                }
            }

            // Send the final batch
            if (eventDataBatch.Count > 0) {
                await _producerClient.SendAsync(eventDataBatch, cancellationToken).NoContext();
                batchCount++;
            }

            _logger?.LogDebug("Successfully produced {Count} messages to stream {Stream} in {BatchCount} batches with partition key {PartitionKey}",
                messageList.Count, stream, batchCount, partitionKey);

            // Acknowledge all messages
            foreach (var message in messageList) {
                await message.Ack<AzureEventHubsProducer>().NoContext();
            }
        } catch (OperationCanceledException) {
            _logger?.LogWarning("Produce operation cancelled for {Count} messages to stream {Stream}", messageList.Count, stream);

            // NACK all messages on cancellation
            foreach (var message in messageList) {
                await message.Nack<AzureEventHubsProducer>($"Produce operation cancelled for stream {stream}", null).NoContext();
            }

            throw;
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to produce {Count} messages to stream {Stream} with partition key {PartitionKey} in {BatchCount} batches",
                messageList.Count, stream, partitionKey, batchCount);

            // NACK all messages
            foreach (var message in messageList) {
                await message.Nack<AzureEventHubsProducer>($"Failed to produce message to stream {stream}", ex).NoContext();
            }

            throw;
        }
    }

    async Task ValidateMessageSizes(
        IList<ProducedMessage> messages,
        StreamName stream,
        AzureEventHubsProduceOptions? options,
        string partitionKey,
        CancellationToken cancellationToken) {
        // Create a test batch to validate message sizes
        var testBatch = await _producerClient.CreateBatchAsync(
            new CreateBatchOptions { PartitionKey = partitionKey },
            cancellationToken
        ).NoContext();

        foreach (var message in messages) {
            var eventData = ToEventData(message, stream, options);

            if (!testBatch.TryAdd(eventData)) {
                // Message is too large for an empty batch
                throw new InvalidOperationException(
                    $"Message {message.MessageId} is too large to fit in an Event Hubs batch. " +
                    $"Consider reducing message size or splitting into smaller messages.");
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    EventData ToEventData(ProducedMessage message, StreamName stream, AzureEventHubsProduceOptions? options) {
        var (eventType, contentType, payload) = _serializer.SerializeEvent(message.Message);

        // Combine message metadata with additional headers
        var combinedMetadata = new Metadata();
        if (message.Metadata != null) {
            foreach (var (key, value) in message.Metadata) {
                combinedMetadata[key] = value;
            }
        }
        if (message.AdditionalHeaders != null) {
            foreach (var (key, value) in message.AdditionalHeaders) {
                combinedMetadata[key] = value;
            }
        }

        var metadataBytes = _metaSerializer.Serialize(combinedMetadata);

        var eventData = new EventData(payload) {
            MessageId = message.MessageId.ToString(),
            ContentType = contentType
        };

        // Add custom properties
        eventData.Properties["EventType"] = eventType;
        eventData.Properties["StreamName"] = stream.ToString();

        if (metadataBytes.Length > 0) {
            eventData.Properties["Metadata"] = Convert.ToBase64String(metadataBytes);
        }

        // Add any additional properties from options
        if (options?.AdditionalProperties != null) {
            foreach (var (key, value) in options.AdditionalProperties) {
                eventData.Properties[key] = value;
            }
        }

        return eventData;
    }

    public async ValueTask DisposeAsync() {
        if (_producerClient != null) {
            await _producerClient.DisposeAsync().ConfigureAwait(false);
        }
        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Options for producing messages to Azure Event Hubs
/// </summary>
public class AzureEventHubsProduceOptions {
    /// <summary>
    /// Partition key for the event. If not specified, the stream name will be used.
    /// </summary>
    public string? PartitionKey { get; set; }

    /// <summary>
    /// Additional properties to include in the event data
    /// </summary>
    public Dictionary<string, object>? AdditionalProperties { get; set; }
}