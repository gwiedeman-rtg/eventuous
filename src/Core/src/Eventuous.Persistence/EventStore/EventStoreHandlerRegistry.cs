// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Collections.Concurrent;

namespace Eventuous;

/// <summary>
/// Registry for managing event store handlers
/// </summary>
public class EventStoreHandlerRegistry {
    readonly ConcurrentDictionary<string, IEventStoreHandler> _handlers = new();

    /// <summary>
    /// Register an event handler
    /// </summary>
    /// <param name="handler">The handler to register</param>
    /// <returns>True if the handler was registered, false if a handler with the same name already exists</returns>
    public bool RegisterHandler(IEventStoreHandler handler) {
        if (handler == null) throw new ArgumentNullException(nameof(handler));
        if (string.IsNullOrWhiteSpace(handler.Name)) throw new ArgumentException("Handler name cannot be null or empty", nameof(handler));

        return _handlers.TryAdd(handler.Name, handler);
    }

    /// <summary>
    /// Unregister an event handler
    /// </summary>
    /// <param name="handlerName">Name of the handler to unregister</param>
    /// <returns>True if the handler was unregistered, false if no handler with that name was found</returns>
    public bool UnregisterHandler(string handlerName) {
        if (string.IsNullOrWhiteSpace(handlerName)) return false;
        
        return _handlers.TryRemove(handlerName, out _);
    }

    /// <summary>
    /// Get all registered handlers
    /// </summary>
    /// <returns>Collection of all registered handlers</returns>
    public IReadOnlyCollection<IEventStoreHandler> GetHandlers() 
        => _handlers.Values.ToArray();

    /// <summary>
    /// Get a specific handler by name
    /// </summary>
    /// <param name="handlerName">Name of the handler</param>
    /// <returns>The handler if found, null otherwise</returns>
    public IEventStoreHandler? GetHandler(string handlerName) {
        _handlers.TryGetValue(handlerName, out var handler);
        return handler;
    }

    /// <summary>
    /// Clear all registered handlers
    /// </summary>
    public void Clear() => _handlers.Clear();

    /// <summary>
    /// Get the count of registered handlers
    /// </summary>
    public int Count => _handlers.Count;
}