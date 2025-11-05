// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

namespace Eventuous.Azure.EventHubs.Versioning;

/// <summary>
/// Strategy for managing stream versions with different concurrency control mechanisms
/// </summary>
public interface IStreamVersionStrategy {
    /// <summary>
    /// Get the current version of a stream
    /// </summary>
    /// <param name="stream">Stream name</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Current version, or null if stream doesn't exist</returns>
    Task<long?> GetVersion(StreamName stream, CancellationToken cancellationToken);

    /// <summary>
    /// Atomically check and increment the stream version
    /// </summary>
    /// <param name="stream">Stream name</param>
    /// <param name="expectedVersion">Expected current version</param>
    /// <param name="eventCount">Number of events being appended</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>New version after increment</returns>
    /// <exception cref="AppendToStreamException">Thrown if expected version doesn't match current version</exception>
    Task<long> IncrementVersion(StreamName stream, long expectedVersion, int eventCount, CancellationToken cancellationToken);
}

