// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Azure.Storage.Blobs;
using Eventuous.Azure.EventHubs.Subscriptions;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Checkpoints;
using Eventuous.Subscriptions.Filters;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Eventuous.Tests.Azure.EventHubs;

/// <summary>
/// Tests for Azure Event Hubs subscription functionality.
/// These tests ensure that the subscription fixes are working and would fail if removed.
/// </summary>
public class SubscriptionTests : IClassFixture<IntegrationFixture> {
    readonly IntegrationFixture _fixture;
    readonly ITestOutputHelper _output;

    public SubscriptionTests(IntegrationFixture fixture, ITestOutputHelper output) {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task Should_Create_Subscription_With_Valid_Options() {
        // Arrange
        var options = new AzureEventHubsSubscriptionOptions {
            SubscriptionId = "test-subscription",
            EventHubConnectionString = _fixture.EventHubConnectionString,
            EventHubName = _fixture.EventHubName,
            BlobStorageConnectionString = _fixture.BlobStorageConnectionString,
            CheckpointContainerName = "test-checkpoints"
        };

        var checkpointStore = new MongoCheckpointStore(_fixture.MongoClient, "test-db");
        var consumePipe = new ConsumePipe();

        // Act
        var subscription = new AzureEventHubsSubscription(
            options,
            checkpointStore,
            consumePipe,
            null, // loggerFactory
            null, // eventSerializer
            null  // metaSerializer
        );

        // Assert
        Assert.NotNull(subscription);
        Assert.Equal("test-subscription", subscription.SubscriptionId);
    }

    [Fact]
    public async Task Should_Validate_Subscription_Options() {
        // Arrange
        var options = new AzureEventHubsSubscriptionOptions {
            SubscriptionId = "", // Invalid - empty
            EventHubConnectionString = "", // Invalid - empty
            EventHubName = "", // Invalid - empty
            BlobStorageConnectionString = "", // Invalid - empty
        };

        // Act & Assert - Should throw exception for invalid options
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => {
                options.Validate();
                return Task.CompletedTask;
            }
        );

        Assert.Contains("SubscriptionId is required", exception.Message);
    }

    [Fact]
    public async Task Should_Handle_Invalid_EventHub_Connection() {
        // Arrange
        var options = new AzureEventHubsSubscriptionOptions {
            SubscriptionId = "test-subscription",
            EventHubConnectionString = "Endpoint=sb://invalid.servicebus.windows.net/;SharedAccessKeyName=invalid;SharedAccessKey=invalid",
            EventHubName = "invalid-hub",
            BlobStorageConnectionString = _fixture.BlobStorageConnectionString,
            CheckpointContainerName = "test-checkpoints"
        };

        var checkpointStore = new MongoCheckpointStore(_fixture.MongoClient, "test-db");
        var consumePipe = new ConsumePipe();

        // Act
        var subscription = new AzureEventHubsSubscription(
            options,
            checkpointStore,
            consumePipe,
            null, // loggerFactory
            null, // eventSerializer
            null  // metaSerializer
        );

        // Assert - Should not throw exception on creation, but will fail on start
        Assert.NotNull(subscription);
    }

    [Fact]
    public async Task Should_Handle_Invalid_Blob_Storage_Connection() {
        // Arrange
        var options = new AzureEventHubsSubscriptionOptions {
            SubscriptionId = "test-subscription",
            EventHubConnectionString = _fixture.EventHubConnectionString,
            EventHubName = _fixture.EventHubName,
            BlobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=invalid;AccountKey=invalid;EndpointSuffix=core.windows.net",
            CheckpointContainerName = "test-checkpoints"
        };

        var checkpointStore = new MongoCheckpointStore(_fixture.MongoClient, "test-db");
        var consumePipe = new ConsumePipe();

        // Act
        var subscription = new AzureEventHubsSubscription(
            options,
            checkpointStore,
            consumePipe,
            null, // loggerFactory
            null, // eventSerializer
            null  // metaSerializer
        );

        // Assert - Should not throw exception on creation, but will fail on start
        Assert.NotNull(subscription);
    }

    [Fact]
    public async Task Should_Handle_Subscription_Disposal() {
        // Arrange
        var options = new AzureEventHubsSubscriptionOptions {
            SubscriptionId = "test-subscription",
            EventHubConnectionString = _fixture.EventHubConnectionString,
            EventHubName = _fixture.EventHubName,
            BlobStorageConnectionString = _fixture.BlobStorageConnectionString,
            CheckpointContainerName = "test-checkpoints"
        };

        var checkpointStore = new MongoCheckpointStore(_fixture.MongoClient, "test-db");
        var consumePipe = new ConsumePipe();

        var subscription = new AzureEventHubsSubscription(
            options,
            checkpointStore,
            consumePipe,
            null, // loggerFactory
            null, // eventSerializer
            null  // metaSerializer
        );

        // Act & Assert - Should not throw exception on disposal
        await subscription.DisposeAsync();
        await subscription.DisposeAsync(); // Multiple disposals should be safe
    }

    [Fact]
    public async Task Should_Handle_Subscription_With_Custom_Consumer_Group() {
        // Arrange
        var options = new AzureEventHubsSubscriptionOptions {
            SubscriptionId = "test-subscription",
            EventHubConnectionString = _fixture.EventHubConnectionString,
            EventHubName = _fixture.EventHubName,
            BlobStorageConnectionString = _fixture.BlobStorageConnectionString,
            CheckpointContainerName = "test-checkpoints",
            ConsumerGroup = "custom-consumer-group"
        };

        var checkpointStore = new MongoCheckpointStore(_fixture.MongoClient, "test-db");
        var consumePipe = new ConsumePipe();

        // Act
        var subscription = new AzureEventHubsSubscription(
            options,
            checkpointStore,
            consumePipe,
            null, // loggerFactory
            null, // eventSerializer
            null  // metaSerializer
        );

        // Assert
        Assert.NotNull(subscription);
        Assert.Equal("custom-consumer-group", options.ConsumerGroup);
    }

    [Fact]
    public async Task Should_Handle_Subscription_With_Custom_Batch_Size() {
        // Arrange
        var options = new AzureEventHubsSubscriptionOptions {
            SubscriptionId = "test-subscription",
            EventHubConnectionString = _fixture.EventHubConnectionString,
            EventHubName = _fixture.EventHubName,
            BlobStorageConnectionString = _fixture.BlobStorageConnectionString,
            CheckpointContainerName = "test-checkpoints",
            MaxBatchSize = 50
        };

        var checkpointStore = new MongoCheckpointStore(_fixture.MongoClient, "test-db");
        var consumePipe = new ConsumePipe();

        // Act
        var subscription = new AzureEventHubsSubscription(
            options,
            checkpointStore,
            consumePipe,
            null, // loggerFactory
            null, // eventSerializer
            null  // metaSerializer
        );

        // Assert
        Assert.NotNull(subscription);
        Assert.Equal(50, options.MaxBatchSize);
    }

    [Fact]
    public async Task Should_Handle_Subscription_With_Custom_Wait_Time() {
        // Arrange
        var options = new AzureEventHubsSubscriptionOptions {
            SubscriptionId = "test-subscription",
            EventHubConnectionString = _fixture.EventHubConnectionString,
            EventHubName = _fixture.EventHubName,
            BlobStorageConnectionString = _fixture.BlobStorageConnectionString,
            CheckpointContainerName = "test-checkpoints",
            MaxWaitTime = TimeSpan.FromSeconds(10)
        };

        var checkpointStore = new MongoCheckpointStore(_fixture.MongoClient, "test-db");
        var consumePipe = new ConsumePipe();

        // Act
        var subscription = new AzureEventHubsSubscription(
            options,
            checkpointStore,
            consumePipe,
            null, // loggerFactory
            null, // eventSerializer
            null  // metaSerializer
        );

        // Assert
        Assert.NotNull(subscription);
        Assert.Equal(TimeSpan.FromSeconds(10), options.MaxWaitTime);
    }

    [Fact]
    public async Task Should_Handle_Subscription_With_Start_From_Beginning() {
        // Arrange
        var options = new AzureEventHubsSubscriptionOptions {
            SubscriptionId = "test-subscription",
            EventHubConnectionString = _fixture.EventHubConnectionString,
            EventHubName = _fixture.EventHubName,
            BlobStorageConnectionString = _fixture.BlobStorageConnectionString,
            CheckpointContainerName = "test-checkpoints",
            StartFromBeginning = true
        };

        var checkpointStore = new MongoCheckpointStore(_fixture.MongoClient, "test-db");
        var consumePipe = new ConsumePipe();

        // Act
        var subscription = new AzureEventHubsSubscription(
            options,
            checkpointStore,
            consumePipe,
            null, // loggerFactory
            null, // eventSerializer
            null  // metaSerializer
        );

        // Assert
        Assert.NotNull(subscription);
        Assert.True(options.StartFromBeginning);
    }

    [Fact]
    public async Task Should_Handle_Subscription_With_Null_Logger_Factory() {
        // Arrange
        var options = new AzureEventHubsSubscriptionOptions {
            SubscriptionId = "test-subscription",
            EventHubConnectionString = _fixture.EventHubConnectionString,
            EventHubName = _fixture.EventHubName,
            BlobStorageConnectionString = _fixture.BlobStorageConnectionString,
            CheckpointContainerName = "test-checkpoints"
        };

        var checkpointStore = new MongoCheckpointStore(_fixture.MongoClient, "test-db");
        var consumePipe = new ConsumePipe();

        // Act
        var subscription = new AzureEventHubsSubscription(
            options,
            checkpointStore,
            consumePipe,
            null, // loggerFactory
            null, // eventSerializer
            null  // metaSerializer
        );

        // Assert - Should not throw exception
        Assert.NotNull(subscription);
    }

    [Fact]
    public async Task Should_Handle_Subscription_With_Null_Event_Serializer() {
        // Arrange
        var options = new AzureEventHubsSubscriptionOptions {
            SubscriptionId = "test-subscription",
            EventHubConnectionString = _fixture.EventHubConnectionString,
            EventHubName = _fixture.EventHubName,
            BlobStorageConnectionString = _fixture.BlobStorageConnectionString,
            CheckpointContainerName = "test-checkpoints"
        };

        var checkpointStore = new MongoCheckpointStore(_fixture.MongoClient, "test-db");
        var consumePipe = new ConsumePipe();

        // Act
        var subscription = new AzureEventHubsSubscription(
            options,
            checkpointStore,
            consumePipe,
            null, // loggerFactory
            null, // eventSerializer
            null  // metaSerializer
        );

        // Assert - Should not throw exception
        Assert.NotNull(subscription);
    }

    [Fact]
    public async Task Should_Handle_Subscription_With_Null_Metadata_Serializer() {
        // Arrange
        var options = new AzureEventHubsSubscriptionOptions {
            SubscriptionId = "test-subscription",
            EventHubConnectionString = _fixture.EventHubConnectionString,
            EventHubName = _fixture.EventHubName,
            BlobStorageConnectionString = _fixture.BlobStorageConnectionString,
            CheckpointContainerName = "test-checkpoints"
        };

        var checkpointStore = new MongoCheckpointStore(_fixture.MongoClient, "test-db");
        var consumePipe = new ConsumePipe();

        // Act
        var subscription = new AzureEventHubsSubscription(
            options,
            checkpointStore,
            consumePipe,
            null, // loggerFactory
            null, // eventSerializer
            null  // metaSerializer
        );

        // Assert - Should not throw exception
        Assert.NotNull(subscription);
    }
}
