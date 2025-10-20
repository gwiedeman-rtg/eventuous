// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Handlers;

namespace Eventuous.Tests.Persistence.Base.Store;

public class PublishingEventStoreTests {
    readonly EventStoreHandlerRegistry _registry;
    readonly PublishingEventStore _eventStore;

    public PublishingEventStoreTests() {
        _registry = new EventStoreHandlerRegistry();
        _eventStore = new PublishingEventStore(_registry, new PublishingEventStoreOptions());
    }

    [Fact]
    public async Task Should_publish_events_to_registered_handlers() {
        // Arrange
        var handledEvents = new List<(StreamName Stream, StreamEvent Event)>();
        var handler = new DelegateEventHandler("TestHandler", (stream, evt) => {
            handledEvents.Add((stream, evt));
        });
        
        _registry.RegisterHandler(handler);

        var streamName = new StreamName("test-stream");
        var events = new[] {
            new NewStreamEvent(Guid.NewGuid(), new TestEvent("Event1"), new {}),
            new NewStreamEvent(Guid.NewGuid(), new TestEvent("Event2"), new {})
        };

        // Act
        await _eventStore.AppendEvents(streamName, ExpectedStreamVersion.NoStream, events, CancellationToken.None);

        // Assert
        Assert.Equal(2, handledEvents.Count);
        Assert.All(handledEvents, x => Assert.Equal(streamName, x.Stream));
        
        var event1 = handledEvents.First(x => ((TestEvent)x.Event.Payload).Message == "Event1");
        var event2 = handledEvents.First(x => ((TestEvent)x.Event.Payload).Message == "Event2");
        
        Assert.NotNull(event1);
        Assert.NotNull(event2);
    }

    [Fact]
    public async Task Should_handle_multiple_handlers() {
        // Arrange
        var handler1Events = new List<StreamEvent>();
        var handler2Events = new List<StreamEvent>();
        
        var handler1 = new DelegateEventHandler("Handler1", (stream, evt) => {
            handler1Events.Add(evt);
        });
        
        var handler2 = new DelegateEventHandler("Handler2", (stream, evt) => {
            handler2Events.Add(evt);
        });

        _registry.RegisterHandler(handler1);
        _registry.RegisterHandler(handler2);

        var streamName = new StreamName("test-stream");
        var events = new[] {
            new NewStreamEvent(Guid.NewGuid(), new TestEvent("Event1"), new {})
        };

        // Act
        await _eventStore.AppendEvents(streamName, ExpectedStreamVersion.NoStream, events, CancellationToken.None);

        // Assert
        Assert.Single(handler1Events);
        Assert.Single(handler2Events);
        Assert.Equal(((TestEvent)handler1Events[0].Payload).Message, "Event1");
        Assert.Equal(((TestEvent)handler2Events[0].Payload).Message, "Event1");
    }

    [Fact]
    public async Task Should_continue_on_handler_failure_when_configured() {
        // Arrange
        var successfulHandlerCalled = false;
        var failingHandler = new DelegateEventHandler("FailingHandler", (stream, evt) => {
            throw new InvalidOperationException("Handler failed");
        });
        
        var successfulHandler = new DelegateEventHandler("SuccessfulHandler", (stream, evt) => {
            successfulHandlerCalled = true;
        });

        _registry.RegisterHandler(failingHandler);
        _registry.RegisterHandler(successfulHandler);

        var options = new PublishingEventStoreOptions { ContinueOnHandlerFailure = true };
        var eventStore = new PublishingEventStore(_registry, options);

        var streamName = new StreamName("test-stream");
        var events = new[] {
            new NewStreamEvent(Guid.NewGuid(), new TestEvent("Event1"), new {})
        };

        // Act & Assert - should not throw
        await eventStore.AppendEvents(streamName, ExpectedStreamVersion.NoStream, events, CancellationToken.None);
        Assert.True(successfulHandlerCalled);
    }

    [Fact]
    public async Task Should_throw_on_handler_failure_when_configured() {
        // Arrange
        var failingHandler = new DelegateEventHandler("FailingHandler", (stream, evt) => {
            throw new InvalidOperationException("Handler failed");
        });

        _registry.RegisterHandler(failingHandler);

        var options = new PublishingEventStoreOptions { ContinueOnHandlerFailure = false };
        var eventStore = new PublishingEventStore(_registry, options);

        var streamName = new StreamName("test-stream");
        var events = new[] {
            new NewStreamEvent(Guid.NewGuid(), new TestEvent("Event1"), new {})
        };

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            eventStore.AppendEvents(streamName, ExpectedStreamVersion.NoStream, events, CancellationToken.None));
    }

    [Fact]
    public void Should_register_and_unregister_handlers() {
        // Arrange
        var handler = new DelegateEventHandler("TestHandler", (stream, evt) => { });

        // Act & Assert
        Assert.True(_registry.RegisterHandler(handler));
        Assert.Equal(1, _registry.Count);
        Assert.Equal(handler, _registry.GetHandler("TestHandler"));

        Assert.True(_registry.UnregisterHandler("TestHandler"));
        Assert.Equal(0, _registry.Count);
        Assert.Null(_registry.GetHandler("TestHandler"));
    }

    [Fact]
    public void Should_not_register_duplicate_handler_names() {
        // Arrange
        var handler1 = new DelegateEventHandler("TestHandler", (stream, evt) => { });
        var handler2 = new DelegateEventHandler("TestHandler", (stream, evt) => { });

        // Act & Assert
        Assert.True(_registry.RegisterHandler(handler1));
        Assert.False(_registry.RegisterHandler(handler2));
        Assert.Equal(1, _registry.Count);
    }

    [Fact]
    public async Task Should_work_as_regular_event_store() {
        // Test that all IEventStore methods work correctly
        var streamName = new StreamName("test-stream");
        var events = new[] {
            new NewStreamEvent(Guid.NewGuid(), new TestEvent("Event1"), new {}),
            new NewStreamEvent(Guid.NewGuid(), new TestEvent("Event2"), new {})
        };

        // Test AppendEvents
        var result = await _eventStore.AppendEvents(streamName, ExpectedStreamVersion.NoStream, events, CancellationToken.None);
        Assert.Equal(1ul, result.GlobalPosition);
        Assert.Equal(1, result.NextExpectedVersion);

        // Test StreamExists
        Assert.True(await _eventStore.StreamExists(streamName, CancellationToken.None));
        Assert.False(await _eventStore.StreamExists(new StreamName("non-existent"), CancellationToken.None));

        // Test ReadEvents
        var readEvents = await _eventStore.ReadEvents(streamName, StreamReadPosition.Start, 10, true, CancellationToken.None);
        Assert.Equal(2, readEvents.Length);
        Assert.Equal("Event1", ((TestEvent)readEvents[0].Payload).Message);
        Assert.Equal("Event2", ((TestEvent)readEvents[1].Payload).Message);

        // Test ReadEventsBackwards
        var backwardEvents = await _eventStore.ReadEventsBackwards(streamName, new StreamReadPosition(1), 2, true, CancellationToken.None);
        Assert.Equal(2, backwardEvents.Length);
        Assert.Equal("Event2", ((TestEvent)backwardEvents[0].Payload).Message);
        Assert.Equal("Event1", ((TestEvent)backwardEvents[1].Payload).Message);
    }

    [Fact]
    public void Should_use_extension_methods() {
        // Test extension methods for fluent configuration
        var handler1 = new DelegateEventHandler("Handler1", (stream, evt) => { });
        var handler2 = new DelegateEventHandler("Handler2", (stream, evt) => { });

        _eventStore
            .RegisterHandler(handler1)
            .RegisterHandler(handler2);

        Assert.Equal(2, _registry.Count);

        _eventStore.UnregisterHandler("Handler1");
        Assert.Equal(1, _registry.Count);
    }

    record TestEvent(string Message);
}