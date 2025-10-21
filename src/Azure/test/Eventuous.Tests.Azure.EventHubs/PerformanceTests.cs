// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using Eventuous.Producers;
using System.Diagnostics;
using Xunit;

namespace Eventuous.Tests.Azure.EventHubs;

/// <summary>
/// Performance tests for Azure Event Hubs Event Store
/// </summary>
[Collection("AzureEventHubs")]
public class PerformanceTests {
    readonly AzureEventHubsFixture _fixture;

    public PerformanceTests(AzureEventHubsFixture fixture) {
        _fixture = fixture;
    }

    [Fact]
    public async Task CanAppendMultipleEventsBatch() {
        // Arrange
        var streamName = new StreamName($"perf-batch-{Guid.NewGuid()}");
        const int eventCount = 100;

        var events = Enumerable.Range(1, eventCount)
            .Select(i => new NewStreamEvent(
                Guid.NewGuid(),
                new TestEvent($"perf-{i}", $"Performance Test Event {i}", DateTime.UtcNow),
                new Metadata { ["EventNumber"] = i, ["BatchTest"] = true }
            ))
            .ToArray();

        var stopwatch = Stopwatch.StartNew();

        // Act
        var result = await _fixture.EventStore.AppendEvents(
            streamName,
            ExpectedStreamVersion.NoStream,
            events
        );

        stopwatch.Stop();

        // Assert
        Assert.True(result.GlobalPosition > 0);
        Assert.Equal(eventCount - 1, result.NextExpectedVersion);

        _fixture.Logger.LogInformation(
            "Appended {EventCount} events in {ElapsedMs}ms ({EventsPerSecond:F2} events/sec)",
            eventCount,
            stopwatch.ElapsedMilliseconds,
            eventCount / stopwatch.Elapsed.TotalSeconds
        );

        // Performance assertion - should be able to append 100 events in reasonable time
        Assert.True(stopwatch.ElapsedMilliseconds < 30000,
            $"Appending {eventCount} events took {stopwatch.ElapsedMilliseconds}ms, which is too slow");
    }

    [Fact]
    public async Task CanProduceMultipleMessagesConcurrently() {
        // Arrange
        var streamName = new StreamName($"perf-concurrent-{Guid.NewGuid()}");
        const int messageCount = 50;
        const int concurrency = 5;

        var allMessages = Enumerable.Range(1, messageCount)
            .Select(i => new ProducedMessage(
                new TestEvent($"concurrent-{i}", $"Concurrent Test Event {i}", DateTime.UtcNow),
                new Metadata { ["EventNumber"] = i, ["ConcurrentTest"] = true }
            ))
            .ToArray();

        // Split messages into batches for concurrent processing
        var batches = allMessages
            .Select((msg, index) => new { msg, index })
            .GroupBy(x => x.index % concurrency)
            .Select(g => g.Select(x => x.msg).ToArray())
            .ToArray();

        var stopwatch = Stopwatch.StartNew();

        // Act
        var tasks = batches.Select(batch =>
            _fixture.Producer.Produce(streamName, batch)
        );

        await Task.WhenAll(tasks);
        stopwatch.Stop();

        // Assert
        _fixture.Logger.LogInformation(
            "Produced {MessageCount} messages concurrently in {ElapsedMs}ms ({MessagesPerSecond:F2} messages/sec)",
            messageCount,
            stopwatch.ElapsedMilliseconds,
            messageCount / stopwatch.Elapsed.TotalSeconds
        );

        // Performance assertion - should be able to produce messages concurrently in reasonable time
        Assert.True(stopwatch.ElapsedMilliseconds < 30000,
            $"Producing {messageCount} messages concurrently took {stopwatch.ElapsedMilliseconds}ms, which is too slow");
    }

    [Fact]
    public async Task CanHandleLargeEvents() {
        // Arrange
        var streamName = new StreamName($"perf-large-{Guid.NewGuid()}");

        // Create a large event (but within Event Hubs limits)
        var largeData = new string('x', 50000); // 50KB string
        var largeEvent = new LargeTestEvent {
            Id = Guid.NewGuid().ToString(),
            LargeData = largeData,
            CreatedAt = DateTime.UtcNow,
            Metadata = Enumerable.Range(1, 100)
                .ToDictionary(i => $"Property{i}", i => $"Value{i}")
        };

        var events = new[] {
            new NewStreamEvent(
                Guid.NewGuid(),
                largeEvent,
                new Metadata { ["Size"] = largeData.Length, ["LargeEventTest"] = true }
            )
        };

        var stopwatch = Stopwatch.StartNew();

        // Act
        var result = await _fixture.EventStore.AppendEvents(
            streamName,
            ExpectedStreamVersion.NoStream,
            events
        );

        stopwatch.Stop();

        // Assert
        Assert.True(result.GlobalPosition > 0);
        Assert.Equal(0, result.NextExpectedVersion);

        _fixture.Logger.LogInformation(
            "Appended large event ({Size} bytes) in {ElapsedMs}ms",
            largeData.Length,
            stopwatch.ElapsedMilliseconds
        );

        // Performance assertion - should be able to handle large events in reasonable time
        Assert.True(stopwatch.ElapsedMilliseconds < 10000,
            $"Appending large event took {stopwatch.ElapsedMilliseconds}ms, which is too slow");
    }

    [Fact]
    public async Task ProducerBatchingWorksEfficiently() {
        // Arrange
        var streamName = new StreamName($"perf-batching-{Guid.NewGuid()}");
        const int messageCount = 200;

        var messages = Enumerable.Range(1, messageCount)
            .Select(i => new ProducedMessage(
                new TestEvent($"batch-{i}", $"Batching Test Event {i}", DateTime.UtcNow),
                new Metadata { ["EventNumber"] = i, ["BatchingTest"] = true }
            ))
            .ToArray();

        var stopwatch = Stopwatch.StartNew();

        // Act - Send all messages in one call to test internal batching
        await _fixture.Producer.Produce(streamName, messages);

        stopwatch.Stop();

        // Assert
        _fixture.Logger.LogInformation(
            "Produced {MessageCount} messages with batching in {ElapsedMs}ms ({MessagesPerSecond:F2} messages/sec)",
            messageCount,
            stopwatch.ElapsedMilliseconds,
            messageCount / stopwatch.Elapsed.TotalSeconds
        );

        // Performance assertion - batching should be efficient
        Assert.True(stopwatch.ElapsedMilliseconds < 30000,
            $"Producing {messageCount} messages with batching took {stopwatch.ElapsedMilliseconds}ms, which is too slow");
    }

    [Fact]
    public async Task MemoryUsageRemainsReasonable() {
        // Arrange
        var streamName = new StreamName($"perf-memory-{Guid.NewGuid()}");
        const int eventCount = 1000;

        var initialMemory = GC.GetTotalMemory(true);

        // Act
        for (int batch = 0; batch < 10; batch++) {
            var events = Enumerable.Range(1, eventCount / 10)
                .Select(i => new NewStreamEvent(
                    Guid.NewGuid(),
                    new TestEvent($"memory-{batch}-{i}", $"Memory Test Event {batch}-{i}", DateTime.UtcNow),
                    new Metadata { ["Batch"] = batch, ["EventNumber"] = i }
                ))
                .ToArray();

            await _fixture.EventStore.AppendEvents(
                streamName,
                batch == 0 ? ExpectedStreamVersion.NoStream : ExpectedStreamVersion.Any,
                events
            );
        }

        // Force garbage collection to get accurate memory reading
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var finalMemory = GC.GetTotalMemory(false);
        var memoryIncrease = finalMemory - initialMemory;

        // Assert
        _fixture.Logger.LogInformation(
            "Memory usage increased by {MemoryIncrease} bytes ({MemoryIncreaseMB:F2} MB) after processing {EventCount} events",
            memoryIncrease,
            memoryIncrease / (1024.0 * 1024.0),
            eventCount
        );

        // Memory assertion - should not leak excessive memory
        // Allow up to 50MB increase for processing 1000 events (this is quite generous)
        Assert.True(memoryIncrease < 50 * 1024 * 1024,
            $"Memory increased by {memoryIncrease / (1024.0 * 1024.0):F2} MB, which might indicate a memory leak");
    }
}