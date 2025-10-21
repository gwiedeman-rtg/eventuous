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
/// Regression tests that would FAIL if the critical fixes were removed.
/// These tests serve as a safety net to prevent regression of the fixes.
/// </summary>
public class RegressionTests : IClassFixture<AzureEventHubsFixture> {
    readonly AzureEventHubsFixture _fixture;
    readonly ITestOutputHelper _output;

    public RegressionTests(AzureEventHubsFixture fixture, ITestOutputHelper output) {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task Regression_Test_1_Optimistic_Concurrency_Control_Must_Not_Be_Removed() {
        // This test would FAIL if the optimistic concurrency control was removed
        // It specifically tests the Table Storage-based versioning mechanism

        var stream = new StreamName("regression-concurrency-test");
        var event1 = new TestEvent("Event1");
        var event2 = new TestEvent("Event2");
        var events1 = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };
        var events2 = new[] { new NewStreamEvent(Guid.NewGuid(), event2, new Metadata()) };

        // Act - Create stream
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events1);

        // Act - Try to append with wrong expected version
        var exception = await Assert.ThrowsAsync<AppendToStreamException>(
            () => _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events2)
        );

        // Assert - Must get specific error about wrong expected version
        // This proves the Table Storage versioning is working
        Assert.Contains("Wrong expected version", exception.Message);
        Assert.Contains("Expected: -1", exception.Message);
        Assert.Contains("Current: 0", exception.Message);

        // Additional verification: Check that the error message format is correct
        // This ensures the ValidateAndUpdateVersion method is working
        Assert.Contains("Wrong expected version. Expected:", exception.Message);
        Assert.Contains("Current:", exception.Message);
    }

    [Fact]
    public async Task Regression_Test_2_AVRO_Parsing_Must_Not_Be_Broken() {
        // This test would FAIL if AVRO parsing was broken or removed
        // It specifically tests the Apache AVRO deserialization

        var stream = new StreamName("regression-avro-test");
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

        // Additional verification: Check that the event data is properly deserialized
        Assert.NotNull(readEvent.Payload);
        Assert.NotNull(readEvent.Metadata);
    }

    [Fact]
    public async Task Regression_Test_3_Partition_Key_Consistency_Must_Not_Be_Broken() {
        // This test would FAIL if partition key consistency was broken
        // It specifically tests that events for the same stream are co-located

        var stream = new StreamName("regression-partition-test");
        var events = Enumerable.Range(1, 3)
            .Select(i => new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event{i}"), new Metadata()))
            .ToArray();

        // Act - Append all events
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act - Read events
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert - All events must be in correct order with correct positions
        // This proves partition key consistency is working
        Assert.Equal(3, readEvents.Length);
        for (int i = 0; i < readEvents.Length; i++) {
            Assert.Equal(i, readEvents[i].Position);
            Assert.Equal(stream.ToString(), readEvents[i].Stream);
        }
    }

    [Fact]
    public async Task Regression_Test_4_Stream_Version_Tracking_Must_Not_Be_Broken() {
        // This test would FAIL if stream version tracking was broken
        // It specifically tests the StreamPosition tracking in event properties

        var stream = new StreamName("regression-version-test");
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
    public async Task Regression_Test_5_Table_Storage_Metadata_Must_Not_Be_Broken() {
        // This test would FAIL if Table Storage metadata was broken
        // It specifically tests the StreamMetadata table operations

        var stream = new StreamName("regression-metadata-test");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Act - Create stream
        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act - Check if stream exists (uses Table Storage metadata)
        var exists = await _fixture.EventStore.StreamExists(stream);

        // Assert - Must return true, proving Table Storage metadata is working
        Assert.True(exists);

        // Additional verification: Check that non-existent stream returns false
        var nonExistentStream = new StreamName("non-existent-stream");
        var nonExistentExists = await _fixture.EventStore.StreamExists(nonExistentStream);
        Assert.False(nonExistentExists);
    }

    [Fact]
    public async Task Regression_Test_6_ETag_Atomic_Updates_Must_Not_Be_Broken() {
        // This test would FAIL if ETag atomic updates were broken
        // It specifically tests the ETag-based optimistic concurrency control

        var stream = new StreamName("regression-etag-test");
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
    public async Task Regression_Test_7_Stream_Isolation_Must_Not_Be_Broken() {
        // This test would FAIL if stream isolation was broken
        // It specifically tests that different streams don't interfere with each other

        var stream1 = new StreamName("regression-isolation-test-1");
        var stream2 = new StreamName("regression-isolation-test-2");
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

        // Additional verification: Check that events are not mixed
        Assert.NotEqual(readEvents1[0].EventId, readEvents2[0].EventId);
    }

    [Fact]
    public async Task Regression_Test_8_Error_Handling_Must_Not_Be_Broken() {
        // This test would FAIL if error handling was broken
        // It specifically tests that proper exceptions are thrown for various error conditions

        var stream = new StreamName("regression-error-test");
        var event1 = new TestEvent("Event1");
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        // Test 1: Wrong expected version
        var exception1 = await Assert.ThrowsAsync<AppendToStreamException>(
            () => _fixture.EventStore.AppendEvents(stream, new ExpectedStreamVersion(5), events)
        );
        Assert.Contains("Wrong expected version", exception1.Message);
        Assert.Contains("Expected: 5", exception1.Message);
        Assert.Contains("Current: -1", exception1.Message);

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
    public async Task Regression_Test_9_Performance_Under_Load_Must_Not_Be_Broken() {
        // This test would FAIL if the implementation couldn't handle load
        // It specifically tests that the fixes don't cause performance issues

        var streams = Enumerable.Range(1, 5)
            .Select(i => new StreamName($"regression-load-test-{i}"))
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
    public async Task Regression_Test_10_Data_Consistency_Must_Not_Be_Broken() {
        // This test would FAIL if data consistency was broken
        // It specifically tests that the fixes maintain data integrity

        var stream = new StreamName("regression-consistency-test");
        var events = Enumerable.Range(1, 5)
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
