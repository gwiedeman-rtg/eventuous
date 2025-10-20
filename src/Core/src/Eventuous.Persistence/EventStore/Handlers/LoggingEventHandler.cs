// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Diagnostics.Tracing;

namespace Eventuous.Handlers;

/// <summary>
/// Simple event handler that logs events using EventSource
/// </summary>
public class LoggingEventHandler : IEventStoreHandler {
    readonly string _name;

    /// <summary>
    /// Create a new LoggingEventHandler
    /// </summary>
    /// <param name="name">Optional name for the handler (defaults to "LoggingEventHandler")</param>
    public LoggingEventHandler(string? name = null) {
        _name = name ?? "LoggingEventHandler";
    }

    /// <inheritdoc />
    public string Name => _name;

    /// <inheritdoc />
    public Task HandleEvent(StreamName streamName, StreamEvent streamEvent, CancellationToken cancellationToken = default) {
        EventHandlerEventSource.Log.EventHandled(
            streamName.ToString(),
            streamEvent.Id.ToString(),
            streamEvent.Payload?.GetType().Name ?? "Unknown",
            streamEvent.Position);

        return Task.CompletedTask;
    }
}

[EventSource(Name = "Eventuous.EventStoreHandlers")]
public class EventHandlerEventSource : EventSource {
    public static readonly EventHandlerEventSource Log = new();

    const int EventHandledId = 1;

    [Event(EventHandledId, Message = "Event handled - Stream: {0}, EventId: {1}, EventType: {2}, Position: {3}", Level = EventLevel.Informational)]
    public void EventHandled(string streamName, string eventId, string eventType, long position)
        => WriteEvent(EventHandledId, streamName, eventId, eventType, position);
}

/// <summary>
/// Simple event handler that delegates to an action
/// </summary>
public class DelegateEventHandler : IEventStoreHandler {
    readonly Func<StreamName, StreamEvent, CancellationToken, Task> _handler;
    readonly string _name;

    /// <summary>
    /// Create a new DelegateEventHandler
    /// </summary>
    /// <param name="name">Name for the handler</param>
    /// <param name="handler">The delegate to handle events</param>
    public DelegateEventHandler(string name, Func<StreamName, StreamEvent, CancellationToken, Task> handler) {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
    }

    /// <summary>
    /// Create a new DelegateEventHandler with synchronous handler
    /// </summary>
    /// <param name="name">Name for the handler</param>
    /// <param name="handler">The delegate to handle events</param>
    public DelegateEventHandler(string name, Action<StreamName, StreamEvent> handler) {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        if (handler == null) throw new ArgumentNullException(nameof(handler));

        _handler = (streamName, streamEvent, _) => {
            handler(streamName, streamEvent);
            return Task.CompletedTask;
        };
    }

    /// <inheritdoc />
    public string Name => _name;

    /// <inheritdoc />
    public Task HandleEvent(StreamName streamName, StreamEvent streamEvent, CancellationToken cancellationToken = default)
        => _handler(streamName, streamEvent, cancellationToken);
}