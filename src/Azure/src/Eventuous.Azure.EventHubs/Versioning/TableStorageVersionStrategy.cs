// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Azure;
using Azure.Data.Tables;

namespace Eventuous.Azure.EventHubs.Versioning;

/// <summary>
/// Version strategy using Azure Table Storage with ETag-based conditional updates for atomic optimistic concurrency
/// </summary>
public class TableStorageVersionStrategy : IStreamVersionStrategy {
    readonly TableClient _tableClient;
    readonly ILogger<TableStorageVersionStrategy>? _logger;
    const string TableName = "StreamVersions";
    const string PartitionKey = "stream-versions";

    public TableStorageVersionStrategy(TableServiceClient tableServiceClient, ILogger<TableStorageVersionStrategy>? logger = null) {
        _tableClient = tableServiceClient.GetTableClient(TableName);
        _logger = logger;

        // Ensure table exists - this is a fire-and-forget call, but we'll handle errors in methods
        _ = EnsureTableExistsAsync();
    }

    async Task EnsureTableExistsAsync() {
        try {
            await _tableClient.CreateIfNotExistsAsync(cancellationToken: default).NoContext();
        } catch {
            // Ignore errors here - we'll handle them when trying to use the table
        }
    }

    public async Task<long?> GetVersion(StreamName stream, CancellationToken cancellationToken) {
        try {
            // Ensure table exists
            await _tableClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).NoContext();

            var response = await _tableClient.GetEntityAsync<TableEntity>(
                PartitionKey,
                stream.ToString(),
                cancellationToken: cancellationToken
            ).NoContext();

            if (response.Value.TryGetValue("Version", out var versionObj) && versionObj is long version) {
                return version;
            }
        } catch (RequestFailedException ex) when (ex.Status == 404) {
            // Stream doesn't exist (entity not found)
            return null;
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to get version for stream {Stream}", stream);
            throw;
        }

        return null;
    }

    public async Task<long> IncrementVersion(StreamName stream, long expectedVersion, int eventCount, CancellationToken cancellationToken) {
        try {
            // Ensure table exists
            await _tableClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).NoContext();

            // Try to get existing entity
            try {
                var response = await _tableClient.GetEntityAsync<TableEntity>(
                    PartitionKey,
                    stream.ToString(),
                    cancellationToken: cancellationToken
                ).NoContext();

                var entity = response.Value;
                long currentVersion;

                // Get version value - handle different types that Azure Tables might return
                if (entity.TryGetValue("Version", out var versionObj)) {
                    currentVersion = versionObj switch {
                        long l => l,
                        int i => i,
                        string s when long.TryParse(s, out var parsed) => parsed,
                        _ => throw new InvalidOperationException($"Invalid version type: {versionObj?.GetType()}")
                    };
                } else {
                    currentVersion = -1;
                }

                if (currentVersion != expectedVersion) {
                    _logger?.LogWarning("Version mismatch for stream {Stream}: expected {Expected}, current {Current}",
                        stream, expectedVersion, currentVersion);
                    throw new AppendToStreamException(stream, new InvalidOperationException(
                        $"WrongExpectedVersion {expectedVersion}, current version {currentVersion}"));
                }

                // Update with ETag for conditional update
                var newVersion = currentVersion + eventCount;
                entity["Version"] = newVersion;

                _logger?.LogDebug("Updating version for stream {Stream} from {Current} to {New}", stream, currentVersion, newVersion);

                await _tableClient.UpdateEntityAsync(
                    entity,
                    entity.ETag,
                    TableUpdateMode.Replace,
                    cancellationToken: cancellationToken
                ).NoContext();

                return newVersion;
            } catch (RequestFailedException ex) when (ex.Status == 404) {
                // Entity doesn't exist, create it if expected version is NoStream
                if (expectedVersion == -1) {
                    // For new stream: version starts at -1 (NoStream), after appending eventCount events, version becomes eventCount - 1
                    // Example: append 1 event to NoStream (-1) -> version becomes 0
                    var newVersion = eventCount - 1;
                    var entity = new TableEntity(PartitionKey, stream.ToString()) {
                        ["Version"] = newVersion
                    };

                    try {
                        await _tableClient.AddEntityAsync(entity, cancellationToken: cancellationToken).NoContext();
                        return newVersion;
                    } catch (RequestFailedException addEx) when (addEx.Status == 404 || addEx.Status == 409) {
                        // Table might not exist, or entity was created concurrently
                        // Try to get the entity that might have been created
                        try {
                            var existingResponse = await _tableClient.GetEntityAsync<TableEntity>(
                                PartitionKey,
                                stream.ToString(),
                                cancellationToken: cancellationToken
                            ).NoContext();
                            var existingEntity = existingResponse.Value;
                            long existingVersion;

                            if (existingEntity.TryGetValue("Version", out var existingVersionObj)) {
                                existingVersion = existingVersionObj switch {
                                    long l => l,
                                    int i => i,
                                    string s when long.TryParse(s, out var parsed) => parsed,
                                    _ => throw new InvalidOperationException($"Invalid version type: {existingVersionObj?.GetType()}")
                                };
                            } else {
                                existingVersion = -1;
                            }

                            if (existingVersion != expectedVersion) {
                                throw new AppendToStreamException(stream, new InvalidOperationException(
                                    $"WrongExpectedVersion {expectedVersion}, current version {existingVersion}"));
                            }
                            // Update the existing entity
                            existingEntity["Version"] = existingVersion + eventCount;
                            await _tableClient.UpdateEntityAsync(
                                existingEntity,
                                existingEntity.ETag,
                                TableUpdateMode.Replace,
                                cancellationToken: cancellationToken
                            ).NoContext();
                            return (long)existingEntity["Version"];
                        } catch {
                            // Re-throw the original exception
                            throw new AppendToStreamException(stream, addEx);
                        }
                    }
                }

                throw new AppendToStreamException(stream, new InvalidOperationException(
                    $"WrongExpectedVersion {expectedVersion}, stream doesn't exist"));
            } catch (RequestFailedException ex) when (ex.Status == 412) {
                // ETag conflict - another process updated
                throw new AppendToStreamException(stream, new InvalidOperationException(
                    $"Concurrent modification detected for stream {stream}"));
            }
        } catch (AppendToStreamException) {
            throw;
        } catch (RequestFailedException ex) {
            _logger?.LogError(ex, "Request failed while incrementing version for stream {Stream}: Status {Status}, Message {Message}",
                stream, ex.Status, ex.Message);
            throw new AppendToStreamException(stream, ex);
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to increment version for stream {Stream}: {ExceptionType} - {Message}",
                stream, ex.GetType().Name, ex.Message);
            throw new AppendToStreamException(stream, ex);
        }
    }
}

