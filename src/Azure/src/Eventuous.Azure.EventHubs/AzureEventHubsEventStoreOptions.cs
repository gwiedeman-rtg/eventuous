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
        
        if (string.IsNullOrWhiteSpace(CaptureContainerName))
            throw new InvalidOperationException("CaptureContainerName is required");
    }
}