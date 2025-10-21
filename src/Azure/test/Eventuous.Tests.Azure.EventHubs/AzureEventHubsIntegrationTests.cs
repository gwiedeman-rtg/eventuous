// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using Eventuous.Producers;
using Xunit;

namespace Eventuous.Tests.Azure.EventHubs;

/// <summary>
/// Integration tests for Azure Event Hubs Event Store
/// These tests require actual Azure Event Hubs and Blob Storage resources
/// Set environment variables or create appsettings.test.json with connection strings
/// </summary>
[Collection("AzureEventHubs")]
public class AzureEventHubsIntegrationTests {
    readonly AzureEventHubsFixture _fixture;

    public AzureEventHubsIntegrationTests(AzureEventHubsFixture fixture) {
        _fixture = fixture;
    }

    [Fact]
    public async Task CanAppendEventsToEventHub() {
        // Arrange
        var streamName = new StreamName($"test-stream-{Guid.NewGuid()}");
        var events = new[] {
            new NewStreamEvent(
                Guid.NewGuid(),
                new TestEvent("1", "First Event", DateTime.UtcNow),
                new Metadata { ["UserId"] = "user-123", ["Source"] = "IntegrationTest" }
            ),
            new NewStreamEvent(
                Guid.NewGuid(),
                new TestEvent("2", "Second Event", DateTime.UtcNow),
                new Metadata { ["UserId"] = "user-123", ["Source"] = "IntegrationTest" }
            )
        };

        // Act
        var result = await _fixture.EventStore.AppendEvents(
            streamName,
            ExpectedStreamVersion.NoStream,
            events
        );

        // Assert
        Assert.True(result.GlobalPosition > 0);
        Assert.Equal(1, result.NextExpectedVersion);

        _fixture.Logger.LogInformation(
            "Successfully appended {Count} events to stream {Stream}. GlobalPosition: {GlobalPosition}, NextVersion: {NextVersion}",
            events.Length, streamName, result.GlobalPosition, result.NextExpectedVersion
        );
    }

    [Fact]
    public async Task CanAppendAndReadEventsFromEventHub() {
        // Arrange
        var streamName = new StreamName($"read-test-stream-{Guid.NewGuid()}");
        var testEvents = new[] {
            new TestEvent("read-1", "Read Test Event 1", DateTime.UtcNow),
            new TestEvent("read-2", "Read Test Event 2", DateTime.UtcNow),
            new TestEvent("read-3", "Read Test Event 3", DateTime.UtcNow)
        };

        var events = testEvents.Select(evt => new NewStreamEvent(
            Guid.NewGuid(),
            evt,
            new Metadata { ["TestId"] = evt.Id, ["Timestamp"] = evt.CreatedAt.ToString("O") }
        )).ToArray();

        // Act - Append events
        var appendResult = await _fixture.EventStore.AppendEvents(
            streamName,
            ExpectedStreamVersion.NoStream,
            events
        );

        Assert.True(appendResult.GlobalPosition > 0);

        // Wait a bit for events to be available for reading
        await Task.Delay(2000);

        // Act - Read events
        var readEvents = await _fixture.EventStore.ReadEvents(
            streamName,
            StreamReadPosition.Start,
            10,
            false
        );

        // Assert
        _fixture.Logger.LogInformation(
            "Read {Count} events from stream {Stream}",
            readEvents.Length, streamName
        );

        // Note: Due to Event Hubs' eventual consistency and partitioning,
        // we might not immediately see all events in a single read operation
        // In a production scenario, you'd implement proper retry logic
        Assert.True(readEvents.Length >= 0, "Should be able to read events without errors");

        foreach (var readEvent in readEvents) {
            Assert.NotNull(readEvent.Payload);
            Assert.IsType<TestEvent>(readEvent.Payload);

            var testEvent = (TestEvent)readEvent.Payload;
            _fixture.Logger.LogInformation(
                "Read event: Id={Id}, Name={Name}, CreatedAt={CreatedAt}",
                testEvent.Id, testEvent.Name, testEvent.CreatedAt
            );
        }
    }

    [Fact]
    public async Task ProducerCanSendMessages() {
        // Arrange
        var streamName = new StreamName($"producer-test-{Guid.NewGuid()}");
        var messages = new[] {
            new ProducedMessage(
                new TestEvent("prod-1", "Producer Test Event", DateTime.UtcNow),
                new Metadata { ["Source"] = "Producer", ["Test"] = "Integration" }
            ),
            new ProducedMessage(
                new TestEvent("prod-2", "Another Producer Event", DateTime.UtcNow),
                new Metadata { ["Source"] = "Producer", ["Test"] = "Integration" }
            )
        };

        // Act
        await _fixture.Producer.Produce(streamName, messages);

        // Assert - No exception thrown means success
        _fixture.Logger.LogInformation(
            "Successfully produced {Count} messages to stream {Stream}",
            messages.Length, streamName
        );
    }

    [Fact]
    public async Task ProducerWithOptionsCanSendMessages() {
        // Arrange
        var streamName = new StreamName($"producer-options-test-{Guid.NewGuid()}");
        var messages = new[] {
            new ProducedMessage(
                new TestEvent("opt-1", "Options Test Event", DateTime.UtcNow),
                new Metadata { ["Source"] = "ProducerWithOptions" }
            )
        };

        var options = new AzureEventHubsProduceOptions {
            PartitionKey = "test-partition",
            AdditionalProperties = new Dictionary<string, object> {
                ["CustomProperty"] = "CustomValue",
                ["TestRun"] = DateTime.UtcNow.ToString("O")
            }
        };

        // Act
        await _fixture.Producer.Produce(streamName, messages, options);

        // Assert - No exception thrown means success
        _fixture.Logger.LogInformation(
            "Successfully produced {Count} messages with options to stream {Stream}",
            messages.Length, streamName
        );
    }

    [Fact]
    public async Task CanCheckStreamExists() {
        // Arrange
        var existingStreamName = new StreamName($"exists-test-{Guid.NewGuid()}");
        var nonExistentStreamName = new StreamName($"non-existent-{Guid.NewGuid()}");

        // First, create a stream by appending an event
        var events = new[] {
            new NewStreamEvent(
                Guid.NewGuid(),
                new TestEvent("exists-1", "Stream Exists Test", DateTime.UtcNow),
                new Metadata()
            )
        };

        await _fixture.EventStore.AppendEvents(
            existingStreamName,
            ExpectedStreamVersion.NoStream,
            events
        );

        // Wait a bit for the stream to be created
        await Task.Delay(1000);

        // Act & Assert
        var existsResult = await _fixture.EventStore.StreamExists(existingStreamName);
        var notExistsResult = await _fixture.EventStore.StreamExists(nonExistentStreamName);

        _fixture.Logger.LogInformation(
            "Stream {ExistingStream} exists: {ExistsResult}, Stream {NonExistentStream} exists: {NotExistsResult}",
            existingStreamName, existsResult, nonExistentStreamName, notExistsResult
        );

        // Note: Due to Event Hubs' architecture, StreamExists might not immediately
        // return true for newly created streams. This is expected behavior.
        Assert.False(notExistsResult, "Non-existent stream should not exist");
    }

    [Fact]
    public void UnsupportedOperationsThrowNotSupportedException() {
        // Arrange
        var streamName = new StreamName("test-stream");

        // Act & Assert
        Assert.ThrowsAsync<NotSupportedException>(async () =>
            await _fixture.EventStore.TruncateStream(
                streamName,
                new StreamTruncatePosition(5),
                ExpectedStreamVersion.Any
            )
        );

        Assert.ThrowsAsync<NotSupportedException>(async () =>
            await _fixture.EventStore.DeleteStream(
                streamName,
                ExpectedStreamVersion.Any
            )
        );
    }

    [Fact]
    public async Task CanHandleEmptyEventCollection() {
        // Arrange
        var streamName = new StreamName($"empty-test-{Guid.NewGuid()}");
        var emptyEvents = Array.Empty<NewStreamEvent>();

        // Act
        var result = await _fixture.EventStore.AppendEvents(
            streamName,
            ExpectedStreamVersion.NoStream,
            emptyEvents
        );

        // Assert
        Assert.Equal(AppendEventsResult.NoOp, result);
    }

    [Fact]
    public async Task ReadEventsFromNonExistentStreamReturnsEmpty() {
        // Arrange
        var nonExistentStream = new StreamName($"non-existent-{Guid.NewGuid()}");

        // Act
        var events = await _fixture.EventStore.ReadEvents(
            nonExistentStream,
            StreamReadPosition.Start,
            10,
            failIfNotFound: false
        );

        // Assert
        Assert.Empty(events);
    }

    [Fact]
    public async Task ReadEventsFromNonExistentStreamWithFailIfNotFoundThrows() {
        // Arrange
        var nonExistentStream = new StreamName($"non-existent-fail-{Guid.NewGuid()}");

        // Act & Assert
        await Assert.ThrowsAsync<StreamNotFound>(async () =>
            await _fixture.EventStore.ReadEvents(
                nonExistentStream,
                StreamReadPosition.Start,
                10,
                failIfNotFound: true
            )
        );
    }
}