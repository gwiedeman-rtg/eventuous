// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Sut.Domain;
using Eventuous.Tests.Persistence.Base.Store;
using Eventuous.Tests.Persistence.Base.Fixtures;
using TUnit.Assertions.AssertConditions.Throws;
using TUnit.Core;
using TUnit.Assertions;

namespace Eventuous.Tests.Azure.EventHubs.Integration.Store;

/// <summary>
/// Integration tests for Azure Event Hubs Event Store using BlobLeaseVersionStrategy
///
/// These tests verify the behavior of the BlobLeaseVersionStrategy which:
/// - Uses Azure Blob Lease for distributed locking during version checks
/// - Provides strong consistency guarantees without requiring Table Storage
/// - Uses blob leases to prevent concurrent modifications
/// - Requires Azure Blob Storage service
///
/// Test Coverage:
/// - Basic append operations (NoStream, sequential appends)
/// - Strong optimistic concurrency control with blob lease locking
/// - Error handling and edge cases
/// - Performance characteristics with distributed locking
///
/// Expected Behavior:
/// - All optimistic concurrency tests should pass reliably
/// - Version validation should always work correctly
/// - No race conditions should occur
/// - Suitable for production scenarios requiring strict consistency without Table Storage
/// </summary>
[ClassDataSource<BlobLeaseVersionStrategyFixture>]
[InheritsTests]
public class BlobLeaseVersionStrategyTests : StoreAppendTests<BlobLeaseVersionStrategyFixture> {
    /// <summary>
    /// Initializes the test class with the BlobLeaseVersionStrategy fixture
    /// </summary>
    /// <param name="fixture">Test fixture configured for BlobLeaseVersionStrategy</param>
    public BlobLeaseVersionStrategyTests(BlobLeaseVersionStrategyFixture fixture) : base(fixture) { }

    // Note: This test class inherits all tests from StoreAppendTests<T> via [InheritsTests] attribute
    // The inherited tests include:
    // - ShouldAppendToNoStream
    // - ShouldAppendOneByOne
    // - ShouldFailOnWrongVersionNoStream
    // - ShouldFailOnWrongVersion
    // - ShouldFailOnWrongVersionWithOptimisticConcurrencyException
    //
    // For BlobLeaseVersionStrategy, all tests should pass reliably due to atomic operations
    // using Azure Blob Lease for distributed locking during version checks.

    /// <summary>
    /// Explicit cleanup to ensure containers are disposed properly
    /// </summary>


}
