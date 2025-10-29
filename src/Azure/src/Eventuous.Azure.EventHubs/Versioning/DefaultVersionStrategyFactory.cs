// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Microsoft.Extensions.Logging;

namespace Eventuous.Azure.EventHubs.Versioning;

/// <summary>
/// Default factory for creating built-in version strategies
/// </summary>
public class DefaultVersionStrategyFactory : IVersionStrategyFactory {
    /// <inheritdoc />
    public IStreamVersionStrategy CreateVersionStrategy(VersionStrategyContext context) {
        if (context.EnableAtomicVersioning) {
            if (context.TableServiceClient != null) {
                return new TableStorageVersionStrategy(
                    context.TableServiceClient,
                    context.LoggerFactory?.CreateLogger<TableStorageVersionStrategy>()
                );
            }

            if (context.BlobServiceClient != null) {
                return new BlobLeaseVersionStrategy(
                    context.BlobServiceClient,
                    context.VersionLockContainerName ?? "eventuous-locks",
                    context.LoggerFactory?.CreateLogger<BlobLeaseVersionStrategy>()
                );
            }

            throw new InvalidOperationException(
                "Atomic versioning requires either TableServiceClient or BlobServiceClient"
            );
        }

        // Non-atomic strategy
        if (context.EventStore == null) {
            throw new InvalidOperationException(
                "NonAtomicVersionStrategy requires AzureEventHubsEventStore instance"
            );
        }

        return new NonAtomicVersionStrategy(
            context.EventStore,
            context.LoggerFactory?.CreateLogger<NonAtomicVersionStrategy>()
        );
    }
}
