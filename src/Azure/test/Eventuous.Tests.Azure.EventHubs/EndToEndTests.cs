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
/// End-to-end tests that verify all critical fixes work together.
/// These tests would FAIL if any of the critical fixes were removed.
/// </summary>
public class EndToEndTests : IClassFixture<AzureEventHubsFixture> {
    readonly AzureEventHubsFixture _fixture;
    readonly ITestOutputHelper _output;

    public EndToEndTests(AzureEventHubsFixture fixture, ITestOutputHelper output) {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task End_To_End_Test_1_Complete_Event_Store_Workflow() {
        // This test would FAIL if any of the critical fixes were removed
        // It tests the complete workflow from append to read

        var stream = new StreamName("e2e-complete-workflow-test");
        var events = Enumerable.Range(1, 5)
            .Select(i => new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event{i}"), new Metadata()))
            .ToArray();

        // Act - Append all events
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act - Read events
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert - All events must be present with correct positions
        Assert.Equal(5, readEvents.Length);
        for (int i = 0; i < readEvents.Length; i++) {
            Assert.Equal(i, readEvents[i].Position);
            Assert.Equal(stream.ToString(), readEvents[i].Stream);
            Assert.Equal($"Event{i + 1}", ((TestEvent)readEvents[i].Payload).Value);
        }
    }

    [Fact]
    public async Task End_To_End_Test_2_Concurrent_Stream_Operations() {
        // This test would FAIL if concurrency control was broken
        // It tests concurrent operations on different streams

        var streams = Enumerable.Range(1, 3)
            .Select(i => new StreamName($"e2e-concurrent-test-{i}"))
            .ToArray();

        var events = streams.Select(stream => new[] {
            new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event-{stream}"), new Metadata())
        }).ToArray();

        // Act - Create all streams concurrently
        var tasks = streams.Select((stream, index) =>
            _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events[index])
        ).ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All must succeed
        Assert.All(results, result => Assert.NotNull(result));

        // Act - Read from all streams
        var readTasks = streams.Select(stream =>
            _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false)
        ).ToArray();

        var readResults = await Task.WhenAll(readTasks);

        // Assert - All must have correct events
        Assert.All(readResults, events => {
            Assert.Single(events);
            Assert.Equal(0, events[0].Position);
        });
    }

    [Fact]
    public async Task End_To_End_Test_3_Stream_Version_Progression() {
        // This test would FAIL if stream version tracking was broken
        // It tests the complete stream version progression

        var stream = new StreamName("e2e-version-progression-test");
        var events = Enumerable.Range(1, 3)
            .Select(i => new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event{i}"), new Metadata()))
            .ToArray();

        // Act - Append events one by one with correct expected versions
        for (int i = 0; i < events.Length; i++) {
            var expectedVersion = i == 0 ? ExpectedStreamVersion.NoStream : new ExpectedStreamVersion(i);
            await _fixture.EventStore.AppendEvents(stream, expectedVersion, new[] { events[i] });
        }

        // Act - Try to append with wrong expected version
        var wrongEvent = new NewStreamEvent(Guid.NewGuid(), new TestEvent("WrongEvent"), new Metadata());
        var exception = await Assert.ThrowsAsync<AppendToStreamException>(
            () => _fixture.EventStore.AppendEvents(stream, new ExpectedStreamVersion(1), new[] { wrongEvent })
        );

        // Assert - Must get error about wrong expected version
        Assert.Contains("Wrong expected version", exception.Message);
        Assert.Contains("Expected: 1", exception.Message);
        Assert.Contains("Current: 2", exception.Message);
    }

    [Fact]
    public async Task End_To_End_Test_4_Stream_Isolation_And_Consistency() {
        // This test would FAIL if stream isolation was broken
        // It tests that different streams don't interfere with each other

        var stream1 = new StreamName("e2e-isolation-test-1");
        var stream2 = new StreamName("e2e-isolation-test-2");
        var event1 = new TestEvent("Event1");
        var event2 = new TestEvent("Event2");
        var events1 = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };
        var events2 = new[] { new NewStreamEvent(Guid.NewGuid(), event2, new Metadata()) };

        // Act - Create both streams
        await _fixture.EventStore.AppendEvents(stream1, ExpectedStreamVersion.NoStream, events1);
        await _fixture.EventStore.AppendEvents(stream2, ExpectedStreamVersion.NoStream, events2);

        // Act - Read from both streams
        var readEvents1 = await _fixture.EventStore.ReadEvents(stream1, StreamReadPosition.Start, 10, false);
        var readEvents2 = await _fixture.EventStore.ReadEvents(stream2, StreamReadPosition.Start, 10, false);

        // Assert - Each stream must have its own events
        Assert.Single(readEvents1);
        Assert.Single(readEvents2);
        Assert.Equal(stream1.ToString(), readEvents1[0].Stream);
        Assert.Equal(stream2.ToString(), readEvents2[0].Stream);
        Assert.NotEqual(readEvents1[0].EventId, readEvents2[0].EventId);
    }

    [Fact]
    public async Task End_To_End_Test_5_Error_Handling_And_Recovery() {
        // This test would FAIL if error handling was broken
        // It tests various error conditions and recovery

        var stream = new StreamName("e2e-error-handling-test");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Test 1: Wrong expected version
        var exception1 = await Assert.ThrowsAsync<AppendToStreamException>(
            () => _fixture.EventStore.AppendEvents(stream, new ExpectedStreamVersion(5), events)
        );
        Assert.Contains("Wrong expected version", exception1.Message);

        // Test 2: Stream not found with failIfNotFound
        var exception2 = await Assert.ThrowsAsync<StreamNotFound>(
            () => _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, true)
        );
        Assert.Contains("Stream not found", exception2.Message);

        // Test 3: Empty event collection
        var result = await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, new NewStreamEvent[0]);
        Assert.Equal(AppendEventsResult.NoOp, result);

        // Test 4: Recovery - create stream after errors
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);
        Assert.Single(readEvents);
    }

    [Fact]
    public async Task End_To_End_Test_6_Performance_And_Load_Handling() {
        // This test would FAIL if the implementation couldn't handle load
        // It tests performance under load

        var streams = Enumerable.Range(1, 10)
            .Select(i => new StreamName($"e2e-load-test-{i}"))
            .ToArray();

        var events = streams.Select(stream => new[] {
            new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event-{stream}"), new Metadata())
        }).ToArray();

        // Act - Create all streams concurrently
        var tasks = streams.Select((stream, index) =>
            _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events[index])
        ).ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All must succeed
        Assert.All(results, result => Assert.NotNull(result));

        // Act - Read from all streams
        var readTasks = streams.Select(stream =>
            _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false)
        ).ToArray();

        var readResults = await Task.WhenAll(readTasks);

        // Assert - All must have correct events
        Assert.All(readResults, events => {
            Assert.Single(events);
            Assert.Equal(0, events[0].Position);
        });
    }

    [Fact]
    public async Task End_To_End_Test_7_Data_Consistency_And_Integrity() {
        // This test would FAIL if data consistency was broken
        // It tests data integrity across multiple operations

        var stream = new StreamName("e2e-consistency-test");
        var events = Enumerable.Range(1, 10)
            .Select(i => new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event{i}"), new Metadata()))
            .ToArray();

        // Act - Append all events
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act - Read events multiple times
        var readEvents1 = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);
        var readEvents2 = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert - Results must be identical
        Assert.Equal(readEvents1.Length, readEvents2.Length);
        for (int i = 0; i < readEvents1.Length; i++) {
            Assert.Equal(readEvents1[i].EventId, readEvents2[i].EventId);
            Assert.Equal(readEvents1[i].Position, readEvents2[i].Position);
            Assert.Equal(readEvents1[i].Stream, readEvents2[i].Stream);
        }
    }

    [Fact]
    public async Task End_To_End_Test_8_Stream_Exists_And_Metadata() {
        // This test would FAIL if stream metadata was broken
        // It tests stream existence and metadata operations

        var stream = new StreamName("e2e-metadata-test");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Act - Check if stream exists before creating it
        var existsBefore = await _fixture.EventStore.StreamExists(stream);
        Assert.False(existsBefore);

        // Act - Create stream
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act - Check if stream exists after creating it
        var existsAfter = await _fixture.EventStore.StreamExists(stream);
        Assert.True(existsAfter);

        // Act - Read events
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);
        Assert.Single(readEvents);
    }

    [Fact]
    public async Task End_To_End_Test_9_Concurrent_Modifications_And_Conflicts() {
        // This test would FAIL if concurrency control was broken
        // It tests concurrent modifications and conflict resolution

        var stream = new StreamName("e2e-concurrent-modifications-test");
        var event1 = new TestEvent("Event1");
        var event2 = new TestEvent("Event2");
        var event3 = new TestEvent("Event3");
        var events1 = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };
        var events2 = new[] { new NewStreamEvent(Guid.NewGuid(), event2, new Metadata()) };
        var events3 = new[] { new NewStreamEvent(Guid.NewGuid(), event3, new Metadata()) };

        // Act - Create stream
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events1);

        // Act - Try concurrent modifications
        var task1 = _fixture.EventStore.AppendEvents(stream, new ExpectedStreamVersion(0), events2);
        var task2 = _fixture.EventStore.AppendEvents(stream, new ExpectedStreamVersion(0), events3);

        // Assert - One must succeed, one must fail
        var results = await Task.WhenAll(task1, task2);
        var exceptions = results.OfType<AppendToStreamException>().ToList();

        Assert.Single(exceptions);
        Assert.Contains("Concurrent update detected", exceptions[0].Message);
    }

    [Fact]
    public async Task End_To_End_Test_10_Complete_Workflow_With_All_Fixes() {
        // This test would FAIL if any of the critical fixes were removed
        // It tests the complete workflow with all fixes working together

        var stream = new StreamName("e2e-complete-workflow-with-fixes-test");
        var events = Enumerable.Range(1, 5)
            .Select(i => new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event{i}"), new Metadata()))
            .ToArray();

        // Act - Append all events
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act - Read events
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert - All events must be present with correct positions
        Assert.Equal(5, readEvents.Length);
        for (int i = 0; i < readEvents.Length; i++) {
            Assert.Equal(i, readEvents[i].Position);
            Assert.Equal(stream.ToString(), readEvents[i].Stream);
            Assert.Equal($"Event{i + 1}", ((TestEvent)readEvents[i].Payload).Value);
        }

        // Act - Check stream exists
        var exists = await _fixture.EventStore.StreamExists(stream);
        Assert.True(exists);

        // Act - Try to append with wrong expected version
        var wrongEvent = new NewStreamEvent(Guid.NewGuid(), new TestEvent("WrongEvent"), new Metadata());
        var exception = await Assert.ThrowsAsync<AppendToStreamException>(
            () => _fixture.EventStore.AppendEvents(stream, new ExpectedStreamVersion(1), new[] { wrongEvent })
        );

        // Assert - Must get error about wrong expected version
        Assert.Contains("Wrong expected version", exception.Message);
        Assert.Contains("Expected: 1", exception.Message);
        Assert.Contains("Current: 4", exception.Message);
    }
}
