// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Runtime.CompilerServices;
using Eventuous.Producers;
using Eventuous.Tools;

namespace Eventuous.Azure.EventHubs;

/// <summary>
/// Azure Event Hubs producer implementation
/// </summary>
public class AzureEventHubsProducer : IProducer<AzureEventHubsProduceOptions> {
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
        var messageList = messages.ToList();
        if (!messageList.Any()) return;

        try {
            var eventDataBatch = await _producerClient.CreateBatchAsync(
                new CreateBatchOptions { PartitionKey = options?.PartitionKey ?? stream.ToString() },
                cancellationToken
            ).NoContext();

            foreach (var message in messageList) {
                var eventData = ToEventData(message, stream, options);
                
                if (!eventDataBatch.TryAdd(eventData)) {
                    // If the batch is full, send it and create a new batch
                    await _producerClient.SendAsync(eventDataBatch, cancellationToken).NoContext();
                    eventDataBatch = await _producerClient.CreateBatchAsync(
                        new CreateBatchOptions { PartitionKey = options?.PartitionKey ?? stream.ToString() },
                        cancellationToken
                    ).NoContext();
                    
                    if (!eventDataBatch.TryAdd(eventData)) {
                        await message.Nack<AzureEventHubsProducer>("Event is too large to fit in a batch", null).NoContext();
                        continue;
                    }
                }
            }

            // Send the final batch
            if (eventDataBatch.Count > 0) {
                await _producerClient.SendAsync(eventDataBatch, cancellationToken).NoContext();
            }

            // Acknowledge all messages
            foreach (var message in messageList) {
                await message.Ack<AzureEventHubsProducer>().NoContext();
            }
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to produce {Count} messages to stream {Stream}", messageList.Count, stream);
            
            // NACK all messages
            foreach (var message in messageList) {
                await message.Nack<AzureEventHubsProducer>($"Failed to produce message to stream {stream}", ex).NoContext();
            }
            
            throw;
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
            ContentType = contentType,
            PartitionKey = options?.PartitionKey ?? stream.ToString()
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