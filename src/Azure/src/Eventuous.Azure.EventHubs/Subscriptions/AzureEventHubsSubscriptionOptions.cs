// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Subscriptions;

namespace Eventuous.Azure.EventHubs.Subscriptions;

/// <summary>
/// Options for Azure Event Hubs subscription
/// </summary>
public record AzureEventHubsSubscriptionOptions : SubscriptionOptions {
    /// <summary>
    /// Event Hub connection string
    /// </summary>
    public string EventHubConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Event Hub name
    /// </summary>
    public string EventHubName { get; set; } = string.Empty;

    /// <summary>
    /// Consumer group name
    /// </summary>
    public string ConsumerGroup { get; set; } = EventHubConsumerClient.DefaultConsumerGroupName;

    /// <summary>
    /// Blob storage connection string for checkpoint storage
    /// </summary>
    public string BlobStorageConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Blob container name for checkpoint storage
    /// </summary>
    public string CheckpointContainerName { get; set; } = "eventhubs-checkpoints";

    /// <summary>
    /// Maximum number of events to process in a batch
    /// </summary>
    public int MaxBatchSize { get; set; } = 100;

    /// <summary>
    /// Maximum wait time for events
    /// </summary>
    public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Whether to start from the beginning of the stream
    /// </summary>
    public bool StartFromBeginning { get; set; } = false;

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
    }
}
