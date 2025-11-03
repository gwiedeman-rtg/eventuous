// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Runtime.Serialization;
using System.Text;
using Eventuous.Diagnostics;
using Eventuous.Producers;
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
            var batch = _container.CreateTransactionalBatch(new PartitionKey(streamId));

            foreach (var doc in eventDocs) {
                batch = batch.CreateItem(doc);
            }

            var batchResponse = await batch.ExecuteAsync(cancellationToken).NoContext();

            if (!batchResponse.IsSuccessStatusCode) {
                var errorMessage = batchResponse.ErrorMessage ?? $"Batch operation failed with status {batchResponse.StatusCode}";
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

    async Task<long?> GetStreamVersion(StreamName stream, CancellationToken cancellationToken) {
        try {
            var streamId = stream.ToString();

            var query = new QueryDefinition(
                "SELECT VALUE MAX(c.streamPosition) FROM c WHERE c.streamId = @streamId"
            ).WithParameter("@streamId", streamId);

            var iterator = _container.GetItemQueryIterator<long>(
                query,
                requestOptions: new QueryRequestOptions {
                    PartitionKey = new PartitionKey(streamId)
                }
            );

            if (iterator.HasMoreResults) {
                var response = await iterator.ReadNextAsync(cancellationToken).NoContext();
                var maxPos   = response.FirstOrDefault();

                // If maxPos is 0 or greater, the stream exists and version is maxPos
                // If no results, stream doesn't exist (return null)
                return maxPos >= 0 ? maxPos : null;
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

