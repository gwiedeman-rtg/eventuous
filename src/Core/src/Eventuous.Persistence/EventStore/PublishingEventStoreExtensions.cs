// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

namespace Eventuous;

/// <summary>
/// Extension methods for PublishingEventStore configuration
/// </summary>
public static class PublishingEventStoreExtensions {
    /// <summary>
    /// Register an event handler with the event store
    /// </summary>
    /// <param name="eventStore">The event store</param>
    /// <param name="handler">The handler to register</param>
    /// <returns>The event store for method chaining</returns>
    public static PublishingEventStore RegisterHandler(this PublishingEventStore eventStore, IEventStoreHandler handler) {
        eventStore.HandlerRegistry.RegisterHandler(handler);
        return eventStore;
    }

    /// <summary>
    /// Register multiple event handlers with the event store
    /// </summary>
    /// <param name="eventStore">The event store</param>
    /// <param name="handlers">The handlers to register</param>
    /// <returns>The event store for method chaining</returns>
    public static PublishingEventStore RegisterHandlers(this PublishingEventStore eventStore, params IEventStoreHandler[] handlers) {
        foreach (var handler in handlers) {
            eventStore.HandlerRegistry.RegisterHandler(handler);
        }
        return eventStore;
    }

    /// <summary>
    /// Register multiple event handlers with the event store
    /// </summary>
    /// <param name="eventStore">The event store</param>
    /// <param name="handlers">The handlers to register</param>
    /// <returns>The event store for method chaining</returns>
    public static PublishingEventStore RegisterHandlers(this PublishingEventStore eventStore, IEnumerable<IEventStoreHandler> handlers) {
        foreach (var handler in handlers) {
            eventStore.HandlerRegistry.RegisterHandler(handler);
        }
        return eventStore;
    }

    /// <summary>
    /// Unregister an event handler from the event store
    /// </summary>
    /// <param name="eventStore">The event store</param>
    /// <param name="handlerName">Name of the handler to unregister</param>
    /// <returns>The event store for method chaining</returns>
    public static PublishingEventStore UnregisterHandler(this PublishingEventStore eventStore, string handlerName) {
        eventStore.HandlerRegistry.UnregisterHandler(handlerName);
        return eventStore;
    }
}