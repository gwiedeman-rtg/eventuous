// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

namespace Eventuous.Azure.EventHubs.Versioning;

/// <summary>
/// Non-atomic version strategy that uses current GetCurrentStreamVersion logic
/// Fastest but subject to race conditions
/// </summary>
public class NonAtomicVersionStrategy : IStreamVersionStrategy {
    readonly AzureEventHubsEventStore _eventStore;
    readonly ILogger<NonAtomicVersionStrategy>? _logger;

    public NonAtomicVersionStrategy(AzureEventHubsEventStore eventStore, ILogger<NonAtomicVersionStrategy>? logger = null) {
        _eventStore = eventStore;
        _logger = logger;
    }

    public Task<long?> GetVersion(StreamName stream, CancellationToken cancellationToken)
        => _eventStore.GetCurrentStreamVersion(stream, cancellationToken);

    public Task<long> IncrementVersion(StreamName stream, long expectedVersion, int eventCount, CancellationToken cancellationToken) {
        // No-op for non-atomic: version is already incremented by the event append
        // Just return what the version would be after appending
        return Task.FromResult(expectedVersion + eventCount);
    }
}

