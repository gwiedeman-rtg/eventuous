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
    }

    public async Task<long?> GetVersion(StreamName stream, CancellationToken cancellationToken) {
        try {
            var response = await _tableClient.GetEntityAsync<TableEntity>(
                PartitionKey,
                stream.ToString(),
                cancellationToken: cancellationToken
            ).NoContext();

            if (response.Value.TryGetValue("Version", out var versionObj) && versionObj is long version) {
                return version;
            }
        } catch (RequestFailedException ex) when (ex.Status == 404) {
            // Stream doesn't exist
            return null;
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to get version for stream {Stream}", stream);
            throw;
        }

        return null;
    }

    public async Task<long> IncrementVersion(StreamName stream, long expectedVersion, int eventCount, CancellationToken cancellationToken) {
        try {
            // Try to get existing entity
            try {
                var response = await _tableClient.GetEntityAsync<TableEntity>(
                    PartitionKey,
                    stream.ToString(),
                    cancellationToken: cancellationToken
                ).NoContext();

                var currentVersion = response.Value.GetInt64("Version") ?? -1;

                if (currentVersion != expectedVersion) {
                    throw new AppendToStreamException(stream, new InvalidOperationException(
                        $"WrongExpectedVersion {expectedVersion}, current version {currentVersion}"));
                }

                // Update with ETag for conditional update
                var entity = response.Value;
                entity["Version"] = currentVersion + eventCount;

                await _tableClient.UpdateEntityAsync(
                    entity,
                    entity.ETag,
                    TableUpdateMode.Replace,
                    cancellationToken: cancellationToken
                ).NoContext();

                return (long)entity["Version"];
            } catch (RequestFailedException ex) when (ex.Status == 404) {
                // Stream doesn't exist, create it if expected version is NoStream
                if (expectedVersion == -1) {
                    var entity = new TableEntity(PartitionKey, stream.ToString()) {
                        ["Version"] = (long)eventCount - 1
                    };

                    await _tableClient.AddEntityAsync(entity, cancellationToken: cancellationToken).NoContext();
                    return eventCount - 1;
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
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to increment version for stream {Stream}", stream);
            throw new AppendToStreamException(stream, ex);
        }
    }
}

