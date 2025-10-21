// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Eventuous.Azure.EventHubs;
using Eventuous.Tests.Persistence.Base;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Eventuous.Tests.Azure.EventHubs;

/// <summary>
/// Tests for edge cases and failure scenarios in Azure Event Hubs Event Store.
/// These tests ensure robust error handling and would fail if fixes are removed.
/// </summary>
public class EdgeCaseTests : IClassFixture<AzureEventHubsFixture> {
    readonly AzureEventHubsFixture _fixture;
    readonly ITestOutputHelper _output;

    public EdgeCaseTests(AzureEventHubsFixture fixture, ITestOutputHelper output) {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task Should_Handle_EventHub_Connection_Failure() {
        // Arrange - Create event store with invalid connection string
        var invalidConnectionString = "Endpoint=sb://invalid.servicebus.windows.net/;SharedAccessKeyName=invalid;SharedAccessKey=invalid";
        var eventStore = new AzureEventHubsEventStore(
            invalidConnectionString,
            "invalid-hub",
            _fixture.BlobStorageConnectionString,
            _fixture.TableStorageConnectionString,
            _fixture.CaptureContainerName
        );

        var stream = new StreamName("connection-failure-stream");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Act & Assert - Should throw exception when connection fails
        var exception = await Assert.ThrowsAsync<AppendToStreamException>(
            () => eventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events)
        );

        Assert.Contains("Failed to append", exception.Message);
    }

    [Fact]
    public async Task Should_Handle_Blob_Storage_Unavailable() {
        // Arrange - Create event store with invalid blob storage connection
        var invalidBlobConnectionString = "DefaultEndpointsProtocol=https;AccountName=invalid;AccountKey=invalid;EndpointSuffix=core.windows.net";
        var eventStore = new AzureEventHubsEventStore(
            _fixture.EventHubConnectionString,
            _fixture.EventHubName,
            invalidBlobConnectionString,
            _fixture.TableStorageConnectionString,
            _fixture.CaptureContainerName
        );

        var stream = new StreamName("blob-unavailable-stream");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Act - Should still work for real-time reading
        await eventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act & Assert - Should fail when trying to read from capture
        var exception = await Assert.ThrowsAsync<Exception>(
            () => eventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false)
        );

        Assert.Contains("Failed to read events from blob", exception.Message);
    }

    [Fact]
    public async Task Should_Handle_Table_Storage_Partition_Unavailable() {
        // Arrange - Create event store with table storage that has no access to specific partition
        var restrictedConnectionString = "DefaultEndpointsProtocol=https;AccountName=restricted;AccountKey=restricted;EndpointSuffix=core.windows.net";
        var eventStore = new AzureEventHubsEventStore(
            _fixture.EventHubConnectionString,
            _fixture.EventHubName,
            _fixture.BlobStorageConnectionString,
            restrictedConnectionString,
            _fixture.CaptureContainerName
        );

        var stream = new StreamName("partition-unavailable-stream");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Act & Assert - Should throw exception when partition is unavailable
        var exception = await Assert.ThrowsAsync<AppendToStreamException>(
            () => eventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events)
        );

        Assert.Contains("Failed to validate and update version", exception.Message);
    }

    [Fact]
    public async Task Should_Handle_Event_Too_Large_For_Batch() {
        // Arrange - Create event with very large payload
        var stream = new StreamName("large-event-stream");
        var largePayload = new string('A', 1024 * 1024); // 1MB string
        var largeEvent = new LargeTestEvent(largePayload);
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), largeEvent, new Metadata()) };

        // Act & Assert - Should throw exception when event is too large
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events)
        );

        Assert.Contains("too large to fit in a batch", exception.Message);
    }

    [Fact]
    public async Task Should_Handle_Empty_Event_Collection() {
        // Arrange
        var stream = new StreamName("empty-events-stream");
        var events = new NewStreamEvent[0];

        // Act
        var result = await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Assert - Should return NoOp result
        Assert.Equal(AppendEventsResult.NoOp, result);
    }

    [Fact]
    public async Task Should_Handle_Null_Event_Collection() {
        // Arrange
        var stream = new StreamName("null-events-stream");

        // Act & Assert - Should throw exception for null events
        var exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, null!)
        );

        Assert.Contains("events", exception.Message);
    }

    [Fact]
    public async Task Should_Handle_Stream_With_Special_Characters() {
        // Arrange
        var stream = new StreamName("stream-with-special-chars-!@#$%^&*()");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Act
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Assert - Should work correctly
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);
        Assert.Single(readEvents);
    }

    [Fact]
    public async Task Should_Handle_Very_Long_Stream_Name() {
        // Arrange
        var longStreamName = new string('A', 1000); // Very long stream name
        var stream = new StreamName(longStreamName);
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Act
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Assert - Should work correctly
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);
        Assert.Single(readEvents);
    }

    [Fact]
    public async Task Should_Handle_Concurrent_Stream_Exists_Checks() {
        // Arrange
        var stream = new StreamName("concurrent-exists-stream");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Act - Check if stream exists before creating it
        var existsBefore = await _fixture.EventStore.StreamExists(stream);
        Assert.False(existsBefore);

        // Create stream
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Check if stream exists after creating it
        var existsAfter = await _fixture.EventStore.StreamExists(stream);
        Assert.True(existsAfter);
    }

    [Fact]
    public async Task Should_Handle_Read_Events_From_Non_Existent_Stream() {
        // Arrange
        var stream = new StreamName("non-existent-read-stream");

        // Act
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert - Should return empty array
        Assert.NotNull(readEvents);
        Assert.Empty(readEvents);
    }

    [Fact]
    public async Task Should_Handle_Read_Events_With_FailIfNotFound_True() {
        // Arrange
        var stream = new StreamName("fail-if-not-found-stream");

        // Act & Assert - Should throw exception when stream doesn't exist
        var exception = await Assert.ThrowsAsync<StreamNotFound>(
            () => _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, true)
        );

        Assert.Contains("Stream not found", exception.Message);
    }

    [Fact]
    public async Task Should_Handle_Read_Events_With_Invalid_Position() {
        // Arrange
        var stream = new StreamName("invalid-position-stream");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act & Assert - Should handle invalid position gracefully
        var readEvents = await _fixture.EventStore.ReadEvents(stream, new StreamReadPosition(999), 10, false);

        // Assert - Should return empty array for position beyond stream
        Assert.NotNull(readEvents);
        Assert.Empty(readEvents);
    }

    [Fact]
    public async Task Should_Handle_Read_Events_With_Zero_Count() {
        // Arrange
        var stream = new StreamName("zero-count-stream");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 0, false);

        // Assert - Should return empty array
        Assert.NotNull(readEvents);
        Assert.Empty(readEvents);
    }

    [Fact]
    public async Task Should_Handle_Read_Events_With_Negative_Count() {
        // Arrange
        var stream = new StreamName("negative-count-stream");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, -1, false);

        // Assert - Should return empty array
        Assert.NotNull(readEvents);
        Assert.Empty(readEvents);
    }

    [Fact]
    public async Task Should_Handle_Read_Events_With_Very_Large_Count() {
        // Arrange
        var stream = new StreamName("large-count-stream");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, int.MaxValue, false);

        // Assert - Should return only available events
        Assert.NotNull(readEvents);
        Assert.Single(readEvents);
    }

    [Fact]
    public async Task Should_Handle_Disposal_After_Use() {
        // Arrange
        var eventStore = new AzureEventHubsEventStore(
            _fixture.EventHubConnectionString,
            _fixture.EventHubName,
            _fixture.BlobStorageConnectionString,
            _fixture.TableStorageConnectionString,
            _fixture.CaptureContainerName
        );

        var stream = new StreamName("disposal-test-stream");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Act
        await eventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);
        eventStore.Dispose();

        // Assert - Should not throw exception on disposal
        Assert.True(true); // If we get here, disposal worked
    }

    [Fact]
    public async Task Should_Handle_Multiple_Disposals() {
        // Arrange
        var eventStore = new AzureEventHubsEventStore(
            _fixture.EventHubConnectionString,
            _fixture.EventHubName,
            _fixture.BlobStorageConnectionString,
            _fixture.TableStorageConnectionString,
            _fixture.CaptureContainerName
        );

        // Act & Assert - Should not throw exception on multiple disposals
        eventStore.Dispose();
        eventStore.Dispose();
        eventStore.Dispose();

        Assert.True(true); // If we get here, multiple disposals worked
    }

    [Fact]
    public async Task Should_Handle_Concurrent_Appends_To_Different_Streams() {
        // Arrange
        var streams = Enumerable.Range(1, 10)
            .Select(i => new StreamName($"concurrent-stream-{i}"))
            .ToArray();

        var events = streams.Select(stream => new[] {
            new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event-{stream}"), new Metadata())
        }).ToArray();

        // Act - Append to all streams concurrently
        var tasks = streams.Select((stream, index) =>
            _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events[index])
        ).ToArray();

        await Task.WhenAll(tasks);

        // Assert - All should succeed
        foreach (var stream in streams) {
            var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);
            Assert.Single(readEvents);
        }
    }
}
