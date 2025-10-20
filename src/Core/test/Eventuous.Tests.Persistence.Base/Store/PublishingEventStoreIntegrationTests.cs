// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Handlers;

namespace Eventuous.Tests.Persistence.Base.Store;

public class PublishingEventStoreIntegrationTests {
    [Fact]
    public async Task Complete_workflow_should_work_end_to_end() {
        // Arrange
        var registry = new EventStoreHandlerRegistry();
        var options = new PublishingEventStoreOptions {
            ContinueOnHandlerFailure = true,
            PublishConcurrently = false
        };
        var eventStore = new PublishingEventStore(registry, options);

        var processedEvents = new List<string>();
        
        // Register multiple handlers
        var auditHandler = new DelegateEventHandler("AuditHandler", (streamName, streamEvent) => {
            processedEvents.Add($"AUDIT:{streamName}:{streamEvent.Id}");
        });
        
        var notificationHandler = new DelegateEventHandler("NotificationHandler", async (streamName, streamEvent, ct) => {
            await Task.Delay(1, ct); // Simulate async work
            processedEvents.Add($"NOTIFICATION:{streamName}:{streamEvent.Id}");
        });

        var loggingHandler = new LoggingEventHandler("TestLogger");

        eventStore
            .RegisterHandler(auditHandler)
            .RegisterHandler(notificationHandler)
            .RegisterHandler(loggingHandler);

        // Create test events
        var streamName = new StreamName("integration-test-stream");
        var eventId1 = Guid.NewGuid();
        var eventId2 = Guid.NewGuid();
        
        var events = new[] {
            new NewStreamEvent(eventId1, new TestEvent("First Event"), new { CorrelationId = Guid.NewGuid() }),
            new NewStreamEvent(eventId2, new TestEvent("Second Event"), new { CorrelationId = Guid.NewGuid() })
        };

        // Act
        var result = await eventStore.AppendEvents(streamName, ExpectedStreamVersion.NoStream, events, CancellationToken.None);

        // Assert - Check event store functionality
        Assert.Equal(1ul, result.GlobalPosition);
        Assert.Equal(1, result.NextExpectedVersion);
        Assert.True(await eventStore.StreamExists(streamName, CancellationToken.None));

        // Assert - Check event publishing
        Assert.Equal(4, processedEvents.Count); // 2 events × 2 handlers (logging handler doesn't add to list)
        Assert.Contains($"AUDIT:{streamName}:{eventId1}", processedEvents);
        Assert.Contains($"AUDIT:{streamName}:{eventId2}", processedEvents);
        Assert.Contains($"NOTIFICATION:{streamName}:{eventId1}", processedEvents);
        Assert.Contains($"NOTIFICATION:{streamName}:{eventId2}", processedEvents);

        // Assert - Check event reading
        var readEvents = await eventStore.ReadEvents(streamName, StreamReadPosition.Start, 10, true, CancellationToken.None);
        Assert.Equal(2, readEvents.Length);
        Assert.Equal("First Event", ((TestEvent)readEvents[0].Payload).Message);
        Assert.Equal("Second Event", ((TestEvent)readEvents[1].Payload).Message);

        // Test handler management
        Assert.Equal(3, registry.Count);
        eventStore.UnregisterHandler("AuditHandler");
        Assert.Equal(2, registry.Count);
        Assert.Null(registry.GetHandler("AuditHandler"));
    }

    [Fact]
    public async Task Should_handle_concurrent_publishing_correctly() {
        // Arrange
        var registry = new EventStoreHandlerRegistry();
        var options = new PublishingEventStoreOptions {
            ContinueOnHandlerFailure = true,
            PublishConcurrently = true // Enable concurrent processing
        };
        var eventStore = new PublishingEventStore(registry, options);

        var processedEvents = new ConcurrentBag<string>();
        
        // Register handlers that simulate different processing times
        var fastHandler = new DelegateEventHandler("FastHandler", async (streamName, streamEvent, ct) => {
            await Task.Delay(1, ct);
            processedEvents.Add($"FAST:{streamEvent.Id}");
        });
        
        var slowHandler = new DelegateEventHandler("SlowHandler", async (streamName, streamEvent, ct) => {
            await Task.Delay(10, ct);
            processedEvents.Add($"SLOW:{streamEvent.Id}");
        });

        eventStore
            .RegisterHandler(fastHandler)
            .RegisterHandler(slowHandler);

        // Create multiple events
        var streamName = new StreamName("concurrent-test-stream");
        var events = Enumerable.Range(1, 5)
            .Select(i => new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event {i}"), new {}))
            .ToArray();

        // Act
        var startTime = DateTime.UtcNow;
        await eventStore.AppendEvents(streamName, ExpectedStreamVersion.NoStream, events, CancellationToken.None);
        var endTime = DateTime.UtcNow;

        // Assert
        Assert.Equal(10, processedEvents.Count); // 5 events × 2 handlers
        
        // With concurrent processing, this should complete faster than sequential processing
        var processingTime = endTime - startTime;
        Assert.True(processingTime.TotalMilliseconds < 100, "Concurrent processing should be faster");
        
        // Verify all events were processed by both handlers
        for (int i = 0; i < events.Length; i++) {
            var eventId = events[i].Id;
            Assert.Contains($"FAST:{eventId}", processedEvents);
            Assert.Contains($"SLOW:{eventId}", processedEvents);
        }
    }

    record TestEvent(string Message);
}