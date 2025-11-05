// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;

namespace Eventuous.Azure.EventHubs.Versioning;

/// <summary>
/// Factory for creating version strategies based on configuration
/// Allows custom strategies to be registered via dependency injection
/// </summary>
public interface IVersionStrategyFactory {
    /// <summary>
    /// Creates a version strategy instance based on the provided context
    /// </summary>
    /// <param name="context">Context containing configuration and dependencies</param>
    /// <returns>Version strategy instance</returns>
    IStreamVersionStrategy CreateVersionStrategy(VersionStrategyContext context);
}

/// <summary>
/// Context for creating version strategies
/// </summary>
public class VersionStrategyContext {
    /// <summary>
    /// The Azure Event Hubs Event Store instance (required for NonAtomicVersionStrategy)
    /// </summary>
    public AzureEventHubsEventStore? EventStore { get; init; }

    /// <summary>
    /// Table Service Client (for TableStorageVersionStrategy)
    /// </summary>
    public TableServiceClient? TableServiceClient { get; init; }

    /// <summary>
    /// Blob Service Client (for BlobLeaseVersionStrategy)
    /// </summary>
    public BlobServiceClient? BlobServiceClient { get; init; }

    /// <summary>
    /// Container name for blob lease strategy
    /// </summary>
    public string? VersionLockContainerName { get; init; }

    /// <summary>
    /// Whether atomic versioning is enabled
    /// </summary>
    public bool EnableAtomicVersioning { get; init; }

    /// <summary>
    /// Logger factory for creating loggers
    /// </summary>
    public ILoggerFactory? LoggerFactory { get; init; }
}
