// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

namespace Eventuous.Azure.EventHubs;

/// <summary>
/// Configuration options for Azure Event Hubs Event Store
/// </summary>
public class AzureEventHubsEventStoreOptions {
    /// <summary>
    /// Event Hub connection string
    /// </summary>
    public string EventHubConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Event Hub name
    /// </summary>
    public string EventHubName { get; set; } = string.Empty;

    /// <summary>
    /// Blob storage connection string for reading captured events
    /// </summary>
    public string BlobStorageConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Table storage connection string for stream metadata and versioning
    /// </summary>
    public string TableStorageConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Whether to enable atomic optimistic concurrency control
    /// When enabled, uses Table Storage (if configured) or Blob Lease for atomic version checking
    /// </summary>
    public bool EnableAtomicVersioning { get; set; } = false;

    /// <summary>
    /// Blob container name for distributed locks when using Blob Lease version strategy
    /// Only used when EnableAtomicVersioning is true and TableStorageConnectionString is not provided
    /// </summary>
    public string VersionLockContainerName { get; set; } = "eventuous-locks";

    /// <summary>
    /// Blob container name where captured events are stored
    /// </summary>
    public string CaptureContainerName { get; set; } = string.Empty;

    /// <summary>
    /// Consumer group for reading events (default: $Default)
    /// </summary>
    public string ConsumerGroup { get; set; } = EventHubConsumerClient.DefaultConsumerGroupName;

    /// <summary>
    /// Whether to use real-time reading from Event Hubs for recent events
    /// </summary>
    public bool UseRealtimeReading { get; set; } = true;

    /// <summary>
    /// Validate the configuration
    /// </summary>
    public void Validate() {
        if (string.IsNullOrWhiteSpace(EventHubConnectionString))
            throw new InvalidOperationException("EventHubConnectionString is required");

        if (string.IsNullOrWhiteSpace(EventHubName))
            throw new InvalidOperationException("EventHubName is required");

        if (string.IsNullOrWhiteSpace(BlobStorageConnectionString))
            throw new InvalidOperationException("BlobStorageConnectionString is required");

        if (EnableAtomicVersioning && string.IsNullOrWhiteSpace(TableStorageConnectionString))
            throw new InvalidOperationException("TableStorageConnectionString is required when EnableAtomicVersioning is true");

        if (string.IsNullOrWhiteSpace(CaptureContainerName))
            throw new InvalidOperationException("CaptureContainerName is required");
    }
}