// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

namespace Eventuous;

/// <summary>
/// Event handler that can be registered with an event store to receive published events
/// </summary>
public interface IEventStoreHandler {
    /// <summary>
    /// Handler name for diagnostics and identification
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Handle an event that was appended to the event store
    /// </summary>
    /// <param name="streamName">Name of the stream the event was appended to</param>
    /// <param name="streamEvent">The event that was appended</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Task representing the handling operation</returns>
    Task HandleEvent(StreamName streamName, StreamEvent streamEvent, CancellationToken cancellationToken = default);
}