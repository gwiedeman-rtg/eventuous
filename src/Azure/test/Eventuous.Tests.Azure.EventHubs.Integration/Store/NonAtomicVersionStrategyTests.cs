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
/// Integration tests for Azure Event Hubs Event Store using NonAtomicVersionStrategy
///
/// These tests verify the behavior of the NonAtomicVersionStrategy which:
/// - Uses GetCurrentStreamVersion() to determine stream versions
/// - May skip version validation if version cannot be determined reliably
/// - Is subject to race conditions but provides better performance
/// - Relies on Event Hubs consumer and blob storage for version detection
///
/// Test Coverage:
/// - Basic append operations (NoStream, sequential appends)
/// - Optimistic concurrency control (may be unreliable due to race conditions)
/// - Error handling and edge cases
/// - Performance characteristics
///
/// Expected Behavior:
/// - ShouldFailOnWrongVersion tests may fail intermittently due to race conditions
/// - This is expected behavior for NonAtomicVersionStrategy
/// - Use atomic versioning strategies for production scenarios requiring strict consistency
/// </summary>
[ClassDataSource<NonAtomicVersionStrategyFixture>]
[InheritsTests]
public class NonAtomicVersionStrategyTests : StoreAppendTests<NonAtomicVersionStrategyFixture> {
    /// <summary>
    /// Initializes the test class with the NonAtomicVersionStrategy fixture
    /// </summary>
    /// <param name="fixture">Test fixture configured for NonAtomicVersionStrategy</param>
    public NonAtomicVersionStrategyTests(NonAtomicVersionStrategyFixture fixture) : base(fixture) { }

    // Note: This test class inherits all tests from StoreAppendTests<T> via [InheritsTests] attribute
    // The inherited tests include:
    // - ShouldAppendToNoStream
    // - ShouldAppendOneByOne
    // - ShouldFailOnWrongVersionNoStream
    // - ShouldFailOnWrongVersion
    // - ShouldFailOnWrongVersionWithOptimisticConcurrencyException
    //
    // For NonAtomicVersionStrategy, some tests (especially ShouldFailOnWrongVersion) may fail
    // intermittently due to race conditions. This is expected behavior for this strategy.
    //
    // CONFIRMED BEHAVIOR:
    // - Event Hubs consumer timeouts cause GetCurrentStreamVersion() to fail
    // - When version detection fails, validation is skipped and appends succeed
    // - This creates race conditions but provides better performance
    // - Use atomic versioning strategies for production scenarios requiring strict consistency

}
