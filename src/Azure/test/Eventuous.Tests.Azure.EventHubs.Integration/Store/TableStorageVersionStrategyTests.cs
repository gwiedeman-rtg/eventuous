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
/// Integration tests for Azure Event Hubs Event Store using TableStorageVersionStrategy
///
/// These tests verify the behavior of the TableStorageVersionStrategy which:
/// - Uses Azure Table Storage with ETag-based conditional updates for atomic optimistic concurrency
/// - Provides strong consistency guarantees
/// - Uses distributed locking via ETags to prevent race conditions
/// - Requires Azure Table Storage service
///
/// Test Coverage:
/// - Basic append operations (NoStream, sequential appends)
/// - Strong optimistic concurrency control with atomic operations
/// - Error handling and edge cases
/// - Performance characteristics with atomic operations
///
/// Expected Behavior:
/// - All optimistic concurrency tests should pass reliably
/// - Version validation should always work correctly
/// - No race conditions should occur
/// - Suitable for production scenarios requiring strict consistency
/// </summary>
[ClassDataSource<TableStorageVersionStrategyFixture>]
[InheritsTests]
public class TableStorageVersionStrategyTests : StoreAppendTests<TableStorageVersionStrategyFixture> {
    /// <summary>
    /// Initializes the test class with the TableStorageVersionStrategy fixture
    /// </summary>
    /// <param name="fixture">Test fixture configured for TableStorageVersionStrategy</param>
    public TableStorageVersionStrategyTests(TableStorageVersionStrategyFixture fixture) : base(fixture) { }

    // Note: This test class inherits all tests from StoreAppendTests<T> via [InheritsTests] attribute
    // The inherited tests include:
    // - ShouldAppendToNoStream
    // - ShouldAppendOneByOne
    // - ShouldFailOnWrongVersionNoStream
    // - ShouldFailOnWrongVersion
    // - ShouldFailOnWrongVersionWithOptimisticConcurrencyException
    //
    // For TableStorageVersionStrategy, all tests should pass reliably due to atomic operations
    // using Azure Table Storage with ETag-based conditional updates.

}
