// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Tests.Persistence.Base.Fixtures;
using Eventuous.Tests.Persistence.Base.Store;
using TUnit.Core;
using TUnit.Core.Helpers;

// ReSharper disable UnusedType.Global

namespace Eventuous.Tests.Azure.EventHubs.Integration.Store;

[InheritsTests]
[ClassDataSource<StoreFixture>]
public class Append(StoreFixture fixture) : StoreAppendTests<StoreFixture>(fixture);

[InheritsTests]
[ClassDataSource<TableStorageVersionStrategyFixture>]
public class Read(TableStorageVersionStrategyFixture fixture) : StoreReadTests<TableStorageVersionStrategyFixture>(fixture) {

    [Test]
    [Category("Store")]
    public async Task ShouldReadOneWaitThenSend(CancellationToken cancellationToken) {
        var stream = Helpers.GetStreamName();

        // Start a background read that polls until the event appears
        var readCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        readCts.CancelAfter(TimeSpan.FromSeconds(30));

        var waitingRead = Task.Run(async () => {
            while (!readCts.IsCancellationRequested) {
                var result = await fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 1, false, readCts.Token);
                if (result.Length > 0) return result;
                await Task.Delay(500, readCts.Token);
            }
            return Array.Empty<StreamEvent>();
        }, readCts.Token);

        // Small delay to ensure the reader is active
        await Task.Delay(200, cancellationToken);

        // Send a single event
        var evt = fixture.CreateEvents(1).First();
        await fixture.AppendEvents(stream, new[] { evt }, ExpectedStreamVersion.NoStream);

        var seen = await waitingRead;
        await Assert.That(seen.Length).IsGreaterThan(0);
        await Assert.That(seen[0].Payload).IsEquivalentTo(evt);
    }
}

[InheritsTests]
[ClassDataSource<StoreFixture>]
public class OtherMethods(StoreFixture fixture) : StoreOtherOpsTests<StoreFixture>(fixture);