// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Blobs.Models;

namespace Eventuous.Azure.EventHubs.Versioning;

/// <summary>
/// Version strategy using Azure Blob Lease for distributed locking during version checks
/// </summary>
public class BlobLeaseVersionStrategy : IStreamVersionStrategy {
    readonly BlobServiceClient _blobServiceClient;
    readonly string _containerName;
    readonly ILogger<BlobLeaseVersionStrategy>? _logger;

    public BlobLeaseVersionStrategy(BlobServiceClient blobServiceClient, string containerName, ILogger<BlobLeaseVersionStrategy>? logger = null) {
        _blobServiceClient = blobServiceClient;
        _containerName = containerName;
        _logger = logger;
    }

    public async Task<long?> GetVersion(StreamName stream, CancellationToken cancellationToken) {
        var blobName = GetBlobName(stream);
        var containerClient = _blobServiceClient.GetBlobContainerClient(_containerName);
        var blobClient = containerClient.GetBlobClient(blobName);

        try {
            if (!await blobClient.ExistsAsync(cancellationToken).NoContext()) {
                return null;
            }

            var properties = await blobClient.GetPropertiesAsync(cancellationToken: cancellationToken).NoContext();

            if (properties.Value.Metadata.TryGetValue("Version", out var versionStr) &&
                long.TryParse(versionStr, out var version)) {
                return version;
            }
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to get version for stream {Stream}", stream);
        }

        return null;
    }

    public async Task<long> IncrementVersion(StreamName stream, long expectedVersion, int eventCount, CancellationToken cancellationToken) {
        var blobName = GetBlobName(stream);
        var containerClient = _blobServiceClient.GetBlobContainerClient(_containerName);
        var blobClient = containerClient.GetBlobClient(blobName);

        // Ensure container exists
        await containerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).NoContext();

        var blobLeaseClient = new BlobLeaseClient(blobClient);
        BlobLease? lease = null;

        try {
            // Ensure blob exists, create if not
            if (!await blobClient.ExistsAsync(cancellationToken).NoContext()) {
                if (expectedVersion == -1) {
                    // NoStream - create blob with version 0
                    await blobClient.UploadAsync(
                        new BinaryData(new byte[0]),
                        new BlobUploadOptions {
                            Metadata = new Dictionary<string, string> { ["Version"] = "-1" }
                        },
                        cancellationToken: cancellationToken
                    ).NoContext();
                } else {
                    throw new AppendToStreamException(stream, new InvalidOperationException(
                        $"WrongExpectedVersion {expectedVersion}, stream doesn't exist"));
                }
            }

            // Acquire lease
            lease = await blobLeaseClient.AcquireAsync(TimeSpan.FromSeconds(60), cancellationToken: cancellationToken).NoContext();

            // Get current version
            var properties = await blobClient.GetPropertiesAsync(
                conditions: new BlobRequestConditions { LeaseId = lease.LeaseId },
                cancellationToken: cancellationToken
            ).NoContext();

            long currentVersion = -1;
            if (properties.Value.Metadata.TryGetValue("Version", out var versionStr) &&
                long.TryParse(versionStr, out currentVersion)) {
                // Version found
            } else {
                currentVersion = -1; // No version metadata means NoStream
            }

            // Validate version
            if (currentVersion != expectedVersion) {
                throw new AppendToStreamException(stream, new InvalidOperationException(
                    $"WrongExpectedVersion {expectedVersion}, current version {currentVersion}"));
            }

            // Update version
            var newVersion = currentVersion == -1 ? eventCount - 1 : currentVersion + eventCount;
            await blobClient.SetMetadataAsync(
                new Dictionary<string, string> { ["Version"] = newVersion.ToString() },
                conditions: new BlobRequestConditions { LeaseId = lease.LeaseId },
                cancellationToken: cancellationToken
            ).NoContext();

            return newVersion;
        } catch (AppendToStreamException) {
            throw;
        } catch (Exception ex) {
            _logger?.LogError(ex, "Failed to increment version for stream {Stream}", stream);
            throw new AppendToStreamException(stream, ex);
        } finally {
            if (lease != null) {
                try {
                    await blobLeaseClient.ReleaseAsync(cancellationToken: cancellationToken).NoContext();
                } catch {
                    // Ignore lease release errors
                }
            }
        }
    }

    static string GetBlobName(StreamName stream) => $"versions/{stream}";
}

