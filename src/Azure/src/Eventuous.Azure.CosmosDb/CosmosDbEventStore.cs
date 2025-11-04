// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Runtime.Serialization;
using System.Text;
using Eventuous.Diagnostics;
using Eventuous.Producers;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using static Eventuous.DeserializationResult;
using static Eventuous.Diagnostics.PersistenceEventSource;

namespace Eventuous.Azure.CosmosDb;

/// <summary>
/// Azure Cosmos DB implementation of <see cref="IEventStore"/>
/// </summary>
public class CosmosDbEventStore : IEventStore, IDisposable {
    readonly CosmosClient                         _cosmosClient;
    readonly Container                            _container;
    readonly Database                             _database;
    readonly IEventSerializer                     _serializer;
    readonly IMetadataSerializer                  _metaSerializer;
    readonly ILogger<CosmosDbEventStore>?         _logger;
    readonly CosmosDbEventStoreOptions            _options;
    readonly string                               _partitionKeyPath;
    bool                                          _disposed;

    /// <summary>
    /// Initialize the event store with Cosmos DB client
    /// </summary>
    public CosmosDbEventStore(
            CosmosClient                          cosmosClient,
            CosmosDbEventStoreOptions             options,
            IEventSerializer?                     serializer     = null,
            IMetadataSerializer?                  metaSerializer = null,
            ILogger<CosmosDbEventStore>?          logger         = null
        ) {
        _cosmosClient     = Ensure.NotNull(cosmosClient);
        _options          = Ensure.NotNull(options);
        _serializer       = serializer     ?? DefaultEventSerializer.Instance;
        _metaSerializer   = metaSerializer ?? DefaultMetadataSerializer.Instance;
        _logger           = logger;
        _partitionKeyPath = _options.PartitionKeyPath ?? "/streamId";

        _database  = _cosmosClient.GetDatabase(_options.Database);
        _container = _database.GetContainer(_options.Container);

        // Verify container exists - batch operations require this
        // Queries might appear to work even if container doesn't exist (returning empty results)
        // but batch writes will fail with 404 if container doesn't exist
        VerifyContainerExists().GetAwaiter().GetResult();

        _logger?.LogInformation(
            "CosmosDB Event Store initialized: Database={Database}, Container={Container}, PartitionKeyPath={PartitionKeyPath}",
            _options.Database, _options.Container, _partitionKeyPath
        );
    }

    /// <inheritdoc/>
    public async Task<bool> StreamExists(StreamName stream, CancellationToken cancellationToken = default) {
        try {
            var query = new QueryDefinition(
                "SELECT VALUE COUNT(1) FROM c WHERE c.streamId = @streamId"
            ).WithParameter("@streamId", stream.ToString());

            var iterator = _container.GetItemQueryIterator<int>(query, requestOptions: new QueryRequestOptions {
                PartitionKey = new PartitionKey(stream.ToString())
            });

            if (iterator.HasMoreResults) {
                var response = await iterator.ReadNextAsync(cancellationToken).NoContext();
                var count    = response.FirstOrDefault();
                return count > 0;
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
            // Get current stream version for validation
            var currentVersion = await GetStreamVersion(stream, cancellationToken).NoContext();

            // Validate expected version
            if (expectedVersion == ExpectedStreamVersion.NoStream) {
                if (currentVersion.HasValue && currentVersion.Value >= 0) {
                    throw new AppendToStreamException(stream, new InvalidOperationException($"WrongExpectedVersion {-1}, stream already exists"));
                }
            } else if (expectedVersion != ExpectedStreamVersion.Any) {
                if (!currentVersion.HasValue) {
                    throw new AppendToStreamException(stream, new InvalidOperationException($"Stream not found for version validation"));
                }

                if (currentVersion.Value != expectedVersion.Value) {
                    throw new AppendToStreamException(stream, new InvalidOperationException($"WrongExpectedVersion {expectedVersion.Value}, current version {currentVersion.Value}"));
                }
            }

            // Prepare events to append
            var streamId       = stream.ToString();
            var nextVersion    = (currentVersion ?? -1) + 1;
            var globalCounter = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            var eventDocs = new List<CosmosEventDocument>();

            foreach (var streamEvent in events) {
                var (eventType, contentType, payload) = _serializer.SerializeEvent(streamEvent.Payload!);
                var metadata                          = _metaSerializer.Serialize(streamEvent.Metadata);

                var eventDoc = new CosmosEventDocument {
                    Id             = streamEvent.Id.ToString(),
                    StreamId       = streamId,
                    MessageType    = eventType,
                    ContentType    = contentType,
                    JsonData       = Encoding.UTF8.GetString(payload),
                    JsonMetadata   = metadata.Length > 0 ? Encoding.UTF8.GetString(metadata) : null,
                    StreamPosition = nextVersion++,
                    GlobalPosition = (ulong)globalCounter++,
                    Created        = DateTime.UtcNow
                };

                eventDocs.Add(eventDoc);
            }

            // Use transactional batch for atomic append (CosmosDB supports up to 100 operations per batch)
            // The partition key must match the partition key path defined in the container (/streamId)
            var partitionKey = new PartitionKey(streamId);
            var batch = _container.CreateTransactionalBatch(partitionKey);

            _logger?.LogDebug(
                "Creating batch for stream {Stream} with partition key {PartitionKey}, {Count} events",
                stream, streamId, eventDocs.Count
            );

            foreach (var doc in eventDocs) {
                // Verify partition key value matches the document's streamId
                if (doc.StreamId != streamId) {
                    throw new InvalidOperationException(
                        $"Partition key mismatch: batch partition key is '{streamId}' but document StreamId is '{doc.StreamId}'"
                    );
                }
                
                // Log the serialized document using System.Text.Json to see what CosmosDB will receive
                // Note: CosmosDB SDK uses System.Text.Json by default, but we need to verify property names match
                var docJson = System.Text.Json.JsonSerializer.Serialize(doc);
                Console.WriteLine($"DEBUG: Adding event to batch - Id={doc.Id}, StreamId={doc.StreamId}, MessageType={doc.MessageType}, StreamPosition={doc.StreamPosition}");
                Console.WriteLine($"DEBUG: Serialized JSON: {docJson}");
                System.Diagnostics.Debug.WriteLine($"DEBUG: Serialized JSON: {docJson}");
                
                _logger?.LogDebug(
                    "Adding event to batch: Id={Id}, StreamId={StreamId}, MessageType={MessageType}, StreamPosition={StreamPosition}, SerializedJson={Json}",
                    doc.Id, doc.StreamId, doc.MessageType, doc.StreamPosition, docJson
                );
                
                batch = batch.CreateItem(doc);
            }

            TransactionalBatchResponse batchResponse;
            try {
                batchResponse = await batch.ExecuteAsync(cancellationToken).NoContext();
            } catch (CosmosException cosmosEx) {
                // Catch CosmosException to get detailed error information
                // Output to console so it shows in test output
                var errorDetails = $"CosmosDB EXCEPTION: StatusCode={cosmosEx.StatusCode}, Message={cosmosEx.Message}, ActivityId={cosmosEx.ActivityId}, ResponseBody={cosmosEx.ResponseBody}";
                Console.WriteLine($"ERROR: {errorDetails}");
                System.Diagnostics.Debug.WriteLine($"ERROR: {errorDetails}");
                
                _logger?.LogError(
                    cosmosEx,
                    "CosmosDB exception during batch operation for stream {Stream}. StatusCode: {StatusCode}, Message: {Message}, ActivityId: {ActivityId}, ResponseBody: {ResponseBody}",
                    stream, cosmosEx.StatusCode, cosmosEx.Message, cosmosEx.ActivityId, cosmosEx.ResponseBody
                );
                throw new AppendToStreamException(stream, new InvalidOperationException(
                    $"CosmosDB batch operation failed: StatusCode={cosmosEx.StatusCode}, Message={cosmosEx.Message}, ActivityId={cosmosEx.ActivityId}, ResponseBody={cosmosEx.ResponseBody}",
                    cosmosEx
                ));
            }

            if (!batchResponse.IsSuccessStatusCode) {
                // Get detailed error information from CosmosDB
                var errorMessage = $"Batch operation failed with status {batchResponse.StatusCode}";
                
                // Try to get more details from the first failed operation if available
                if (batchResponse.Count > 0) {
                    var firstResult = batchResponse[0];
                    if (!firstResult.IsSuccessStatusCode) {
                        errorMessage = $"{errorMessage}. First operation error: StatusCode={firstResult.StatusCode}";
                    }
                }
                
                // Output to console so it shows in test output
                Console.WriteLine($"ERROR: Batch operation failed for stream {stream}. Status: {batchResponse.StatusCode}, Error: {errorMessage}, RequestCharge: {batchResponse.RequestCharge}");
                System.Diagnostics.Debug.WriteLine($"ERROR: Batch operation failed: {errorMessage}");
                
                _logger?.LogError(
                    "Batch operation failed for stream {Stream}. Status: {Status}, Error: {Error}, RequestCharge: {RequestCharge}",
                    stream, batchResponse.StatusCode, errorMessage, batchResponse.RequestCharge
                );
                
                throw new AppendToStreamException(stream, new InvalidOperationException(errorMessage));
            }

            var nextExpectedVersion = nextVersion - 1;
            var globalPosition     = (ulong)globalCounter - 1;

            _logger?.LogInformation(
                "Successfully appended {Count} events to stream {Stream}, next version {Version}",
                events.Count, stream, nextExpectedVersion
            );

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
            var streamId = stream.ToString();

            var query = new QueryDefinition(
                "SELECT * FROM c WHERE c.streamId = @streamId AND c.streamPosition >= @start ORDER BY c.streamPosition OFFSET 0 LIMIT @count"
            )
                .WithParameter("@streamId", streamId)
                .WithParameter("@start", start.Value)
                .WithParameter("@count", count);

            var iterator = _container.GetItemQueryIterator<CosmosEventDocument>(
                query,
                requestOptions: new QueryRequestOptions {
                    PartitionKey = new PartitionKey(streamId)
                }
            );

            var events = new List<StreamEvent>();

            while (iterator.HasMoreResults && events.Count < count) {
                var response = await iterator.ReadNextAsync(cancellationToken).NoContext();

                foreach (var doc in response) {
                    var streamEvent = ToStreamEvent(doc);
                    events.Add(streamEvent);
                }
            }

            if (events.Count == 0 && failIfNotFound) {
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
            var streamId = stream.ToString();

            // For backwards read, we need to get events in reverse order
            var query = new QueryDefinition(
                "SELECT * FROM c WHERE c.streamId = @streamId AND c.streamPosition <= @start ORDER BY c.streamPosition DESC OFFSET 0 LIMIT @count"
            )
                .WithParameter("@streamId", streamId)
                .WithParameter("@start", start.Value == long.MaxValue ? long.MaxValue : start.Value)
                .WithParameter("@count", count);

            var iterator = _container.GetItemQueryIterator<CosmosEventDocument>(
                query,
                requestOptions: new QueryRequestOptions {
                    PartitionKey = new PartitionKey(streamId)
                }
            );

            var events = new List<StreamEvent>();

            while (iterator.HasMoreResults && events.Count < count) {
                var response = await iterator.ReadNextAsync(cancellationToken).NoContext();

                foreach (var doc in response) {
                    var streamEvent = ToStreamEvent(doc);
                    events.Add(streamEvent);
                }
            }

            if (events.Count == 0 && failIfNotFound) {
                throw new StreamNotFound(stream);
            }

            return events.Take(count).ToArray();
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
    public async Task TruncateStream(
            StreamName             stream,
            StreamTruncatePosition truncatePosition,
            ExpectedStreamVersion  expectedVersion,
            CancellationToken      cancellationToken = default
        ) {
        try {
            var streamId = stream.ToString();

            // Delete events after truncate position
            var query = new QueryDefinition(
                "SELECT c.id, c.streamPosition FROM c WHERE c.streamId = @streamId AND c.streamPosition > @position"
            )
                .WithParameter("@streamId", streamId)
                .WithParameter("@position", truncatePosition.Value);

            var iterator = _container.GetItemQueryIterator<CosmosEventDocument>(
                query,
                requestOptions: new QueryRequestOptions {
                    PartitionKey = new PartitionKey(streamId)
                }
            );

            var batch = _container.CreateTransactionalBatch(new PartitionKey(streamId));
            var batchOperationCount = 0;

            while (iterator.HasMoreResults) {
                var response = await iterator.ReadNextAsync(cancellationToken).NoContext();

                foreach (var doc in response) {
                    batch = batch.DeleteItem(doc.Id);
                    batchOperationCount++;
                }

                if (batchOperationCount > 0) {
                    await batch.ExecuteAsync(cancellationToken).NoContext();
                    batch = _container.CreateTransactionalBatch(new PartitionKey(streamId));
                    batchOperationCount = 0;
                }
            }

            _logger?.LogInformation("Truncated stream {Stream} at position {Position}", stream, truncatePosition.Value);
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to truncate stream {Stream} at position {Position}", stream, truncatePosition.Value);
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task DeleteStream(
            StreamName            stream,
            ExpectedStreamVersion expectedVersion,
            CancellationToken     cancellationToken = default
        ) {
        try {
            var streamId = stream.ToString();

            // Delete all events for the stream
            var query = new QueryDefinition(
                "SELECT c.id FROM c WHERE c.streamId = @streamId"
            ).WithParameter("@streamId", streamId);

            var iterator = _container.GetItemQueryIterator<CosmosEventDocument>(
                query,
                requestOptions: new QueryRequestOptions {
                    PartitionKey = new PartitionKey(streamId)
                }
            );

            var batch = _container.CreateTransactionalBatch(new PartitionKey(streamId));
            var batchOperationCount = 0;

            while (iterator.HasMoreResults) {
                var response = await iterator.ReadNextAsync(cancellationToken).NoContext();

                foreach (var doc in response) {
                    batch = batch.DeleteItem(doc.Id);
                    batchOperationCount++;
                }

                if (batchOperationCount > 0) {
                    await batch.ExecuteAsync(cancellationToken).NoContext();
                    batch = _container.CreateTransactionalBatch(new PartitionKey(streamId));
                    batchOperationCount = 0;
                }
            }

            _logger?.LogInformation("Deleted stream {Stream}", stream);
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to delete stream {Stream}", stream);
            throw;
        }
    }

    async Task VerifyContainerExists() {
        try {
            // Try to read container metadata - this will throw if container doesn't exist
            await _container.ReadContainerAsync();
            _logger?.LogDebug("Container {Container} verified to exist", _options.Container);
        } catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound) {
            _logger?.LogError(
                "Container '{Container}' does not exist in database '{Database}'. " +
                "Ensure the container has been created with partition key path '{PartitionKeyPath}'. " +
                "This error typically occurs when initialization failed silently.",
                _options.Container, _options.Database, _partitionKeyPath
            );
            throw new InvalidOperationException(
                $"Container '{_options.Container}' does not exist in database '{_options.Database}'. " +
                $"Ensure the container has been created with partition key path '{_partitionKeyPath}'.",
                ex
            );
        }
    }

    async Task<long?> GetStreamVersion(StreamName stream, CancellationToken cancellationToken) {
        try {
            var streamId = stream.ToString();

            // First check if the stream has any events
            // This is necessary because MAX() on an empty set can return confusing results
            var countQuery = new QueryDefinition(
                "SELECT VALUE COUNT(1) FROM c WHERE c.streamId = @streamId"
            ).WithParameter("@streamId", streamId);

            var countIterator = _container.GetItemQueryIterator<int>(
                countQuery,
                requestOptions: new QueryRequestOptions {
                    PartitionKey = new PartitionKey(streamId)
                }
            );

            int count = 0;
            if (countIterator.HasMoreResults) {
                var countResponse = await countIterator.ReadNextAsync(cancellationToken).NoContext();
                count = countResponse.FirstOrDefault();

                // If count is 0, stream doesn't exist
                if (count == 0) {
                    return null;
                }
            }

            // Stream exists, get the max position
            var maxQuery = new QueryDefinition(
                "SELECT VALUE MAX(c.streamPosition) FROM c WHERE c.streamId = @streamId"
            ).WithParameter("@streamId", streamId);

            var maxIterator = _container.GetItemQueryIterator<long>(
                maxQuery,
                requestOptions: new QueryRequestOptions {
                    PartitionKey = new PartitionKey(streamId)
                }
            );

            if (maxIterator.HasMoreResults) {
                var response = await maxIterator.ReadNextAsync(cancellationToken).NoContext();
                var maxPos   = response.FirstOrDefault();
                
                // Verify data integrity: max position should be count - 1
                // Positions start at 0, so if we have 3 events, max position should be 2
                var expectedMaxPos = count - 1;
                if (maxPos != expectedMaxPos) {
                    _logger?.LogWarning(
                        "Stream version inconsistency detected for {Stream}: max position is {MaxPos} but expected {ExpectedMaxPos} based on count {Count}. This may indicate missing or duplicate events.",
                        stream, maxPos, expectedMaxPos, count
                    );
                }
                
                return maxPos;
            }

            return null;
        } catch (Exception ex) {
            _logger?.LogWarning(ex, "Failed to get stream version for {Stream}", stream);
            return null;
        }
    }

    StreamEvent ToStreamEvent(CosmosEventDocument doc) {
        var payloadBytes = Encoding.UTF8.GetBytes(doc.JsonData);
        var deserialized = _serializer.DeserializeEvent(payloadBytes, doc.MessageType, doc.ContentType);

        var meta = string.IsNullOrEmpty(doc.JsonMetadata)
            ? new Metadata()
            : _metaSerializer.Deserialize(Encoding.UTF8.GetBytes(doc.JsonMetadata));

        return deserialized switch {
            SuccessfullyDeserialized success => new StreamEvent(
                Guid.Parse(doc.Id),
                success.Payload,
                meta ?? new Metadata(),
                doc.ContentType,
                doc.StreamPosition
            ),
            FailedToDeserialize failed => throw new SerializationException($"Can't deserialize {doc.MessageType}: {failed.Error}"),
            _                          => throw new InvalidOperationException("Unknown deserialization result")
        };
    }

    public void Dispose() {
        if (_disposed) return;

        _cosmosClient.Dispose();
        _disposed = true;
    }
}

