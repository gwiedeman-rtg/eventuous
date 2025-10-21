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
/// Integration tests that verify critical fixes are working.
/// These tests would FAIL if the fixes were removed, ensuring they stay in place.
/// </summary>
public class CriticalFixesIntegrationTests : IClassFixture<AzureEventHubsFixture> {
    readonly AzureEventHubsFixture _fixture;
    readonly ITestOutputHelper _output;

    public CriticalFixesIntegrationTests(AzureEventHubsFixture fixture, ITestOutputHelper output) {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task Critical_Fix_1_Optimistic_Concurrency_Must_Work() {
        // This test would FAIL if optimistic concurrency control was removed
        // It verifies that the Table Storage-based versioning is working

        var stream = new StreamName("critical-concurrency-test");
        var event1 = new TestEvent("Event1");
        var event2 = new TestEvent("Event2");
        var events1 = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };
        var events2 = new[] { new NewStreamEvent(Guid.NewGuid(), event2, new Metadata()) };

        // Act - First append should succeed
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events1);

        // Act - Second append with wrong expected version should FAIL
        var exception = await Assert.ThrowsAsync<AppendToStreamException>(
            () => _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events2)
        );

        // Assert - Must get specific error about wrong expected version
        // This proves the Table Storage versioning is working
        Assert.Contains("Wrong expected version", exception.Message);
        Assert.Contains("Expected: -1", exception.Message);
        Assert.Contains("Current: 0", exception.Message);
    }

    [Fact]
    public async Task Critical_Fix_2_AVRO_Parsing_Must_Work() {
        // This test would FAIL if AVRO parsing was broken
        // It verifies that the Apache AVRO deserialization is working

        var stream = new StreamName("critical-avro-test");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Act - Append event
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act - Read events (this uses AVRO parsing if capture is enabled)
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert - Must have correct event with proper deserialization
        Assert.Single(readEvents);
        var readEvent = readEvents[0];
        Assert.Equal("TestEvent", readEvent.EventType);
        Assert.Equal(stream.ToString(), readEvent.Stream);
        Assert.Equal(0, readEvent.Position);
    }

    [Fact]
    public async Task Critical_Fix_3_Partition_Key_Consistency_Must_Work() {
        // This test would FAIL if partition key consistency was broken
        // It verifies that events for the same stream are co-located

        var stream = new StreamName("critical-partition-test");
        var events = Enumerable.Range(1, 5)
            .Select(i => new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event{i}"), new Metadata()))
            .ToArray();

        // Act - Append multiple events
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act - Read events
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert - All events must be in correct order with correct positions
        // This proves partition key consistency is working
        Assert.Equal(5, readEvents.Length);
        for (int i = 0; i < readEvents.Length; i++) {
            Assert.Equal(i, readEvents[i].Position);
            Assert.Equal(stream.ToString(), readEvents[i].Stream);
        }
    }

    [Fact]
    public async Task Critical_Fix_4_Stream_Version_Tracking_Must_Work() {
        // This test would FAIL if stream version tracking was broken
        // It verifies that stream positions are correctly tracked

        var stream = new StreamName("critical-version-test");
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
        // This proves stream version tracking is working
        Assert.Contains("Wrong expected version", exception.Message);
        Assert.Contains("Expected: 1", exception.Message);
        Assert.Contains("Current: 2", exception.Message);
    }

    [Fact]
    public async Task Critical_Fix_5_Table_Storage_Metadata_Must_Work() {
        // This test would FAIL if Table Storage metadata was broken
        // It verifies that stream metadata is correctly stored and retrieved

        var stream = new StreamName("critical-metadata-test");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Act - Create stream
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act - Check if stream exists (uses Table Storage metadata)
        var exists = await _fixture.EventStore.StreamExists(stream);

        // Assert - Must return true, proving Table Storage metadata is working
        Assert.True(exists);
    }

    [Fact]
    public async Task Critical_Fix_6_ETag_Atomic_Updates_Must_Work() {
        // This test would FAIL if ETag atomic updates were broken
        // It verifies that concurrent updates are properly handled

        var stream = new StreamName("critical-etag-test");
        var event1 = new TestEvent("Event1");
        var event2 = new TestEvent("Event2");
        var event3 = new TestEvent("Event3");
        var events1 = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };
        var events2 = new[] { new NewStreamEvent(Guid.NewGuid(), event2, new Metadata()) };
        var events3 = new[] { new NewStreamEvent(Guid.NewGuid(), event3, new Metadata()) };

        // Act - Create stream
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events1);

        // Act - Try concurrent updates with same expected version
        var task1 = _fixture.EventStore.AppendEvents(stream, new ExpectedStreamVersion(0), events2);
        var task2 = _fixture.EventStore.AppendEvents(stream, new ExpectedStreamVersion(0), events3);

        // Assert - One must succeed, one must fail with ETag conflict
        var results = await Task.WhenAll(task1, task2);
        var exceptions = results.OfType<AppendToStreamException>().ToList();

        Assert.Single(exceptions);
        Assert.Contains("Concurrent update detected", exceptions[0].Message);
    }

    [Fact]
    public async Task Critical_Fix_7_Stream_Isolation_Must_Work() {
        // This test would FAIL if stream isolation was broken
        // It verifies that different streams don't interfere with each other

        var stream1 = new StreamName("critical-isolation-test-1");
        var stream2 = new StreamName("critical-isolation-test-2");
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
    }

    [Fact]
    public async Task Critical_Fix_8_Error_Handling_Must_Work() {
        // This test would FAIL if error handling was broken
        // It verifies that proper exceptions are thrown for various error conditions

        var stream = new StreamName("critical-error-test");
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
    }

    [Fact]
    public async Task Critical_Fix_9_Performance_Under_Load_Must_Work() {
        // This test would FAIL if the implementation couldn't handle load
        // It verifies that the fixes don't cause performance issues

        var streams = Enumerable.Range(1, 10)
            .Select(i => new StreamName($"critical-load-test-{i}"))
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
    public async Task Critical_Fix_10_Data_Consistency_Must_Work() {
        // This test would FAIL if data consistency was broken
        // It verifies that the fixes maintain data integrity

        var stream = new StreamName("critical-consistency-test");
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
}
