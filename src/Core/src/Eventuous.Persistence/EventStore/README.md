# PublishingEventStore

The `PublishingEventStore` is an implementation of `IEventStore` that extends the functionality of an in-memory event store by adding event publishing capabilities. When events are appended to the store, they are automatically published to registered event handlers.

## Features

- **Event Publishing**: Automatically publishes events to registered handlers after they are appended
- **Handler Registration**: Support for registering and unregistering event handlers through configuration
- **Error Handling**: Configurable behavior for handling failures in event handlers
- **Concurrent Processing**: Option to process handlers concurrently or sequentially
- **In-Memory Storage**: Based on the existing `InMemoryEventStore` for testing and development scenarios

## Basic Usage

```csharp
// Create handler registry and event store
var registry = new EventStoreHandlerRegistry();
var eventStore = new PublishingEventStore(registry);

// Create and register handlers
var auditHandler = new DelegateEventHandler("AuditHandler", (streamName, streamEvent) => {
    Console.WriteLine($"AUDIT: Event {streamEvent.Id} appended to stream {streamName}");
});

eventStore.RegisterHandler(auditHandler);

// Append events - handlers will be called automatically
var streamName = new StreamName("user-123");
var events = new[] {
    new NewStreamEvent(Guid.NewGuid(), new UserRegistered("john@example.com"), new {})
};

await eventStore.AppendEvents(streamName, ExpectedStreamVersion.NoStream, events, CancellationToken.None);
```

## Configuration Options

The `PublishingEventStoreOptions` class provides configuration options:

```csharp
var options = new PublishingEventStoreOptions {
    ContinueOnHandlerFailure = true,  // Continue processing if a handler fails (default: true)
    PublishConcurrently = false       // Process handlers concurrently (default: false)
};

var eventStore = new PublishingEventStore(registry, options);
```

## Handler Types

### DelegateEventHandler
Simple handler that delegates to an action:

```csharp
var handler = new DelegateEventHandler("MyHandler", (streamName, streamEvent) => {
    // Handle the event
});
```

### LoggingEventHandler
Handler that logs events using EventSource:

```csharp
var handler = new LoggingEventHandler("EventLogger");
```

### Custom Handlers
Implement the `IEventStoreHandler` interface:

```csharp
public class MyCustomHandler : IEventStoreHandler {
    public string Name => "MyCustomHandler";

    public Task HandleEvent(StreamName streamName, StreamEvent streamEvent, CancellationToken cancellationToken = default) {
        // Custom event handling logic
        return Task.CompletedTask;
    }
}
```

## Extension Methods

The `PublishingEventStoreExtensions` class provides fluent configuration methods:

```csharp
eventStore
    .RegisterHandler(handler1)
    .RegisterHandler(handler2)
    .UnregisterHandler("handler1");
```

## Error Handling

The event store uses EventSource for diagnostics and logging. Handler failures are logged through the `PersistenceEventSource`.

## Thread Safety

The `PublishingEventStore` and `EventStoreHandlerRegistry` are thread-safe and can be used concurrently from multiple threads.