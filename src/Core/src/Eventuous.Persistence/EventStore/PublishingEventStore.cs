// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Collections.Concurrent;
using Eventuous.Diagnostics;

namespace Eventuous;

/// <summary>
/// Event store that publishes events to registered handlers after they are appended
/// Based on InMemoryEventStore but adds event publishing capabilities
/// </summary>
public class PublishingEventStore : IEventStore {
    readonly ConcurrentDictionary<StreamName, InMemoryStream> _storage = new();
    readonly List<StreamEvent> _global = [];
    readonly EventStoreHandlerRegistry _handlerRegistry;
    readonly PublishingEventStoreOptions _options;

    /// <summary>
    /// Create a new PublishingEventStore
    /// </summary>
    /// <param name="handlerRegistry">Registry for event handlers</param>
    /// <param name="options">Configuration options</param>
    public PublishingEventStore(
        EventStoreHandlerRegistry handlerRegistry, 
        PublishingEventStoreOptions? options = null) {
        _handlerRegistry = handlerRegistry ?? throw new ArgumentNullException(nameof(handlerRegistry));
        _options = options ?? new PublishingEventStoreOptions();
    }

    /// <summary>
    /// Get the handler registry for this event store
    /// </summary>
    public EventStoreHandlerRegistry HandlerRegistry => _handlerRegistry;

    /// <inheritdoc />
    public Task<bool> StreamExists(StreamName streamName, CancellationToken cancellationToken)
        => Task.FromResult(_storage.ContainsKey(streamName));

    /// <inheritdoc />
    public async Task<AppendEventsResult> AppendEvents(
        StreamName stream,
        ExpectedStreamVersion expectedVersion,
        IReadOnlyCollection<NewStreamEvent> events,
        CancellationToken cancellationToken) {
        
        var existing = _storage.GetOrAdd(stream, s => new(s));
        existing.AppendEvents(expectedVersion, events);
        
        var streamEvents = events.Select((x, i) => new StreamEvent(x.Id, x.Payload, x.Metadata, "application/json", _global.Count + i)).ToArray();
        _global.AddRange(streamEvents);

        var result = new AppendEventsResult((ulong)(_global.Count - 1), existing.Version);

        // Publish events to registered handlers
        await PublishEvents(stream, streamEvents, cancellationToken);

        return result;
    }

    /// <inheritdoc />
    public Task<StreamEvent[]> ReadEvents(StreamName stream, StreamReadPosition start, int count, bool failIfNotFound, CancellationToken cancellationToken)
        => Task.FromResult(FindStream(stream, failIfNotFound).GetEvents(start, count).ToArray());

    /// <inheritdoc />
    public Task<StreamEvent[]> ReadEventsBackwards(StreamName stream, StreamReadPosition start, int count, bool failIfNotFound, CancellationToken cancellationToken)
        => Task.FromResult(FindStream(stream, failIfNotFound).GetEventsBackwards(start, count).ToArray());

    /// <inheritdoc />
    public Task TruncateStream(
        StreamName stream,
        StreamTruncatePosition truncatePosition,
        ExpectedStreamVersion expectedVersion,
        CancellationToken cancellationToken) {
        
        FindStream(stream, expectedVersion.ExistingStream).Truncate(expectedVersion, truncatePosition);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task DeleteStream(StreamName stream, ExpectedStreamVersion expectedVersion, CancellationToken cancellationToken) {
        var existing = FindStream(stream, expectedVersion.ExistingStream);
        existing.CheckVersion(expectedVersion);
        _storage.Remove(stream, out _);

        return Task.CompletedTask;
    }

    InMemoryStream FindStream(StreamName stream, bool failIfNotFound)
        => !_storage.TryGetValue(stream, out var existing)
            ? failIfNotFound
                ? throw new StreamNotFound(stream)
                : new(stream)
            : existing;

    async Task PublishEvents(StreamName streamName, StreamEvent[] events, CancellationToken cancellationToken) {
        var handlers = _handlerRegistry.GetHandlers();
        if (!handlers.Any()) return;

        var publishingTasks = new List<Task>();

        foreach (var streamEvent in events) {
            foreach (var handler in handlers) {
                if (_options.PublishConcurrently) {
                    publishingTasks.Add(PublishEventToHandler(handler, streamName, streamEvent, cancellationToken));
                } else {
                    await PublishEventToHandler(handler, streamName, streamEvent, cancellationToken);
                }
            }
        }

        if (_options.PublishConcurrently && publishingTasks.Any()) {
            await Task.WhenAll(publishingTasks);
        }
    }

    async Task PublishEventToHandler(IEventStoreHandler handler, StreamName streamName, StreamEvent streamEvent, CancellationToken cancellationToken) {
        try {
            await handler.HandleEvent(streamName, streamEvent, cancellationToken);
        } catch (Exception ex) {
            PersistenceEventSource.Log.UnableToPublishEvent(streamName, handler.Name, ex);
            
            if (!_options.ContinueOnHandlerFailure) {
                throw;
            }
        }
    }
}

/// <summary>
/// Configuration options for PublishingEventStore
/// </summary>
public class PublishingEventStoreOptions {
    /// <summary>
    /// Whether to continue processing if a handler fails (default: true)
    /// </summary>
    public bool ContinueOnHandlerFailure { get; set; } = true;

    /// <summary>
    /// Whether to publish events to handlers concurrently (default: false)
    /// </summary>
    public bool PublishConcurrently { get; set; } = false;
}

// Reuse the InMemoryStream class from the original InMemoryEventStore
record StoredEvent(StreamEvent Event, int Position);

class InMemoryStream(StreamName name) {
    public int Version { get; private set; } = -1;
    public string Name { get; } = name;

    readonly List<StoredEvent> _events = [];

    public void CheckVersion(ExpectedStreamVersion expectedVersion) {
        if (expectedVersion != ExpectedStreamVersion.Any && expectedVersion.Value != Version) 
            throw new WrongVersion(expectedVersion, Version);
    }

    public void AppendEvents(ExpectedStreamVersion expectedVersion, IReadOnlyCollection<NewStreamEvent> events) {
        CheckVersion(expectedVersion);

        foreach (var newEvent in events) {
            var version = ++Version;
            var streamEvent = new StreamEvent(newEvent.Id, newEvent.Payload, newEvent.Metadata, "application/json", version);
            _events.Add(new(streamEvent, version));
        }
    }

    public IEnumerable<StreamEvent> GetEvents(StreamReadPosition from, int count) {
        var selected = _events.SkipWhile(x => x.Position < from.Value);

        if (count > 0) selected = selected.Take(count);

        return selected.Select(x => x.Event with { Position = x.Position });
    }

    public IEnumerable<StreamEvent> GetEventsBackwards(StreamReadPosition from, int count) {
        var position = (int)from.Value;

        while (count-- > 0) {
            yield return _events[position--].Event;
        }
    }

    public void Truncate(ExpectedStreamVersion version, StreamTruncatePosition position) {
        CheckVersion(version);
        _events.RemoveAll(x => x.Position <= position.Value);
    }
}

public class WrongVersion(ExpectedStreamVersion expected, int actual) : Exception($"Wrong stream version. Expected {expected.Value}, actual {actual}");