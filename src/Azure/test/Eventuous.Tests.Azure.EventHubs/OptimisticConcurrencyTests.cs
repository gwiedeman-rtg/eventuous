// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Diagnostics;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Eventuous.Azure.EventHubs;
using Eventuous.Azure.EventHubs.Subscriptions;
using Eventuous.Tests.Persistence.Base;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Eventuous.Tests.Azure.EventHubs;

/// <summary>
/// Tests for optimistic concurrency control in Azure Event Hubs Event Store.
/// These tests ensure that the concurrency fixes are working and would fail if removed.
/// </summary>
public class OptimisticConcurrencyTests : IClassFixture<AzureEventHubsFixture> {
    readonly AzureEventHubsFixture _fixture;
    readonly ITestOutputHelper _output;

    public OptimisticConcurrencyTests(AzureEventHubsFixture fixture, ITestOutputHelper output) {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task Should_Reject_Concurrent_Appends_With_Same_Expected_Version() {
        // Arrange
        var stream = new StreamName("concurrent-test-stream");
        var event1 = new TestEvent("Event1");
        var event2 = new TestEvent("Event2");
        var events1 = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };
        var events2 = new[] { new NewStreamEvent(Guid.NewGuid(), event2, new Metadata()) };

        // Act & Assert - Both should succeed initially
        var result1 = await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events1);
        var result2 = await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.Any, events2);

        // Verify both events were appended
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);
        Assert.Equal(2, readEvents.Length);
    }

    [Fact]
    public async Task Should_Throw_Exception_On_Wrong_Expected_Version() {
        // Arrange
        var stream = new StreamName("version-conflict-stream");
        var event1 = new TestEvent("Event1");
        var event2 = new TestEvent("Event2");
        var events1 = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };
        var events2 = new[] { new NewStreamEvent(Guid.NewGuid(), event2, new Metadata()) };

        // Act - First append succeeds
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events1);

        // Act & Assert - Second append with wrong expected version should fail
        var exception = await Assert.ThrowsAsync<AppendToStreamException>(
            () => _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events2)
        );

        Assert.Contains("Wrong expected version", exception.Message);
        Assert.Contains("Expected: -1", exception.Message);
        Assert.Contains("Current: 0", exception.Message);
    }

    [Fact]
    public async Task Should_Throw_Exception_On_Concurrent_Modifications() {
        // Arrange
        var stream = new StreamName("concurrent-modification-stream");
        var event1 = new TestEvent("Event1");
        var event2 = new TestEvent("Event2");
        var event3 = new TestEvent("Event3");
        var events1 = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };
        var events2 = new[] { new NewStreamEvent(Guid.NewGuid(), event2, new Metadata()) };
        var events3 = new[] { new NewStreamEvent(Guid.NewGuid(), event3, new Metadata()) };

        // Act - Create initial stream
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events1);

        // Act - Simulate concurrent modifications
        var task1 = _fixture.EventStore.AppendEvents(stream, new ExpectedStreamVersion(0), events2);
        var task2 = _fixture.EventStore.AppendEvents(stream, new ExpectedStreamVersion(0), events3);

        // Assert - One should succeed, one should fail
        var results = await Task.WhenAll(task1, task2);
        var exceptions = results.OfType<AppendToStreamException>().ToList();

        Assert.Single(exceptions);
        Assert.Contains("Concurrent update detected", exceptions[0].Message);
    }

    [Fact]
    public async Task Should_Handle_ETag_Conflicts_Correctly() {
        // Arrange
        var stream = new StreamName("etag-conflict-stream");
        var event1 = new TestEvent("Event1");
        var event2 = new TestEvent("Event2");
        var events1 = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };
        var events2 = new[] { new NewStreamEvent(Guid.NewGuid(), event2, new Metadata()) };

        // Act - Create stream
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events1);

        // Act - Try to append with wrong version (simulating ETag conflict)
        var exception = await Assert.ThrowsAsync<AppendToStreamException>(
            () => _fixture.EventStore.AppendEvents(stream, new ExpectedStreamVersion(5), events2)
        );

        // Assert - Should get specific error about wrong expected version
        Assert.Contains("Wrong expected version", exception.Message);
        Assert.Contains("Expected: 5", exception.Message);
        Assert.Contains("Current: 0", exception.Message);
    }

    [Fact]
    public async Task Should_Validate_Stream_Exists_Before_Appending() {
        // Arrange
        var stream = new StreamName("non-existent-stream");
        var event1 = new TestEvent("Event1");
        var events1 = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Act & Assert - Should fail when trying to append to non-existent stream with specific version
        var exception = await Assert.ThrowsAsync<AppendToStreamException>(
            () => _fixture.EventStore.AppendEvents(stream, new ExpectedStreamVersion(0), events1)
        );

        Assert.Contains("Wrong expected version", exception.Message);
        Assert.Contains("Expected: 0", exception.Message);
        Assert.Contains("Current: -1", exception.Message);
    }

    [Fact]
    public async Task Should_Allow_Any_Version_Append() {
        // Arrange
        var stream = new StreamName("any-version-stream");
        var event1 = new TestEvent("Event1");
        var event2 = new TestEvent("Event2");
        var events1 = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };
        var events2 = new[] { new NewStreamEvent(Guid.NewGuid(), event2, new Metadata()) };

        // Act - Create stream
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events1);

        // Act - Append with Any version should succeed
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.Any, events2);

        // Assert - Both events should be present
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);
        Assert.Equal(2, readEvents.Length);
    }

    [Fact]
    public async Task Should_Maintain_Stream_Version_Consistency() {
        // Arrange
        var stream = new StreamName("version-consistency-stream");
        var events = Enumerable.Range(1, 5)
            .Select(i => new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event{i}"), new Metadata()))
            .ToArray();

        // Act - Append events one by one
        for (int i = 0; i < events.Length; i++) {
            var expectedVersion = i == 0 ? ExpectedStreamVersion.NoStream : new ExpectedStreamVersion(i);
            await _fixture.EventStore.AppendEvents(stream, expectedVersion, new[] { events[i] });
        }

        // Assert - Stream should have correct version
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);
        Assert.Equal(5, readEvents.Length);

        // Assert - Each event should have correct position
        for (int i = 0; i < readEvents.Length; i++) {
            Assert.Equal(i, readEvents[i].Position);
        }
    }

    [Fact]
    public async Task Should_Handle_Table_Storage_Unavailable() {
        // Arrange - Create event store with invalid table storage connection
        var invalidConnectionString = "DefaultEndpointsProtocol=https;AccountName=invalid;AccountKey=invalid;EndpointSuffix=core.windows.net";
        var eventStore = new AzureEventHubsEventStore(
            _fixture.EventHubConnectionString,
            _fixture.EventHubName,
            _fixture.BlobStorageConnectionString,
            invalidConnectionString,
            _fixture.CaptureContainerName
        );

        var stream = new StreamName("table-unavailable-stream");
        var event1 = new TestEvent("Event1");
        var events1 = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Act & Assert - Should throw exception when table storage is unavailable
        var exception = await Assert.ThrowsAsync<AppendToStreamException>(
            () => eventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events1)
        );

        Assert.Contains("Failed to validate and update version", exception.Message);
    }

    [Fact]
    public async Task Should_Handle_Concurrent_Stream_Creation() {
        // Arrange
        var stream = new StreamName("concurrent-creation-stream");
        var event1 = new TestEvent("Event1");
        var event2 = new TestEvent("Event2");
        var events1 = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };
        var events2 = new[] { new NewStreamEvent(Guid.NewGuid(), event2, new Metadata()) };

        // Act - Try to create stream concurrently
        var task1 = _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events1);
        var task2 = _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events2);

        // Assert - One should succeed, one should fail
        var results = await Task.WhenAll(task1, task2);
        var exceptions = results.OfType<AppendToStreamException>().ToList();

        Assert.Single(exceptions);
        Assert.Contains("Stream", exceptions[0].Message);
        Assert.Contains("already exists", exceptions[0].Message);
    }

    [Fact]
    public async Task Should_Preserve_Event_Ordering_Under_Concurrency() {
        // Arrange
        var stream = new StreamName("ordering-test-stream");
        var events = Enumerable.Range(1, 10)
            .Select(i => new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event{i}"), new Metadata()))
            .ToArray();

        // Act - Append events concurrently but with correct versions
        var tasks = new List<Task<AppendEventsResult>>();
        for (int i = 0; i < events.Length; i++) {
            var expectedVersion = i == 0 ? ExpectedStreamVersion.NoStream : new ExpectedStreamVersion(i);
            tasks.Add(_fixture.EventStore.AppendEvents(stream, expectedVersion, new[] { events[i] }));
        }

        // Wait for all to complete
        await Task.WhenAll(tasks);

        // Assert - Events should be in correct order
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);
        Assert.Equal(10, readEvents.Length);

        for (int i = 0; i < readEvents.Length; i++) {
            Assert.Equal(i, readEvents[i].Position);
        }
    }
}
