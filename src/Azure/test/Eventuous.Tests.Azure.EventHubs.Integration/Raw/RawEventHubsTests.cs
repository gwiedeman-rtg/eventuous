// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using Eventuous.Tests.Azure.EventHubs.Integration.Store;
using Eventuous.Tests.Persistence.Base.Fixtures;
using TUnit.Assertions;
using TUnit.Core;

namespace Eventuous.Tests.Azure.EventHubs.Integration.Raw;

/// <summary>
/// Raw tests to validate Event Hubs emulator send/receive without any framework layers.
/// Uses the same emulator container as other fixtures.
/// </summary>
[ClassDataSource<TableStorageVersionStrategyFixture>]
public class RawEventHubsTests(TableStorageVersionStrategyFixture fixture) {
    const string HubName       = "test-hub";     // Matches fixture setup
    const string ConsumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

    [Test]
    public async Task ShouldSendAndReceive_WithReadEventsAsync(CancellationToken cancellationToken) {
        var connectionString = fixture.Container.GetConnectionString();

        await using var producer = new EventHubProducerClient(connectionString, HubName);
        await using var consumer = new EventHubConsumerClient(ConsumerGroup, connectionString, HubName);

        var stream  = Helpers.GetStreamName().ToString();
        var payload = new BinaryData("raw-test");

        // Start consumer first: ReadEventsAsync (all partitions), look for our StreamName property
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        var readTask = Task.Run(async () => {
            await foreach (var ev in consumer.ReadEventsAsync(cts.Token)) {
                if (ev.Data is null) continue;
                if (ev.Data.Properties.TryGetValue("StreamName", out var s) && s?.ToString() == stream)
                    return ev.Data;
            }
            return (EventData?)null;
        }, cts.Token);

        // Small delay to ensure reader loop started
        await Task.Delay(200, cancellationToken);

        // Send one event with the expected properties and a partition key to stabilize routing
        var eventData = new EventData(payload) {
            MessageId   = Guid.NewGuid().ToString(),
            ContentType = "text/plain"
        };
        eventData.Properties["EventType"]  = "Raw.Test";
        eventData.Properties["StreamName"] = stream;

        using var batch = await producer.CreateBatchAsync(new CreateBatchOptions { PartitionKey = stream }, cancellationToken);
        if (!batch.TryAdd(eventData)) throw new InvalidOperationException("Failed to add event to batch");
        await producer.SendAsync(batch, cancellationToken);

        var received = await readTask;
        await Assert.That(received).IsNotNull();
        await Assert.That(received!.MessageId).IsNotEmpty();
    }

    [Test]
    public async Task ShouldSendAndReceive_FromPartition(CancellationToken cancellationToken) {
        var connectionString = fixture.Container.GetConnectionString();

        await using var producer = new EventHubProducerClient(connectionString, HubName);
        await using var consumer = new EventHubConsumerClient(ConsumerGroup, connectionString, HubName);

        var stream  = Helpers.GetStreamName().ToString();
        var payload = new BinaryData("raw-partition-test");

        // Determine partitions
        var partitions = await consumer.GetPartitionIdsAsync(cancellationToken);
        await Assert.That(partitions.Length).IsGreaterThan(0);

        // Start per-partition readers first
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        var readers = partitions.Select(async pid => {
            await foreach (var ev in consumer.ReadEventsFromPartitionAsync(pid, EventPosition.Earliest, cancellationToken: cts.Token)) {
                if (ev.Data is null) continue;
                if (ev.Data.Properties.TryGetValue("StreamName", out var s) && s?.ToString() == stream)
                    return ev.Data;
            }
            return (EventData?)null;
        }).ToArray();

        // Small delay to ensure readers are running
        await Task.Delay(200, cancellationToken);

        // Send one event with a partition key (stream) so it deterministically lands on one partition
        var eventData = new EventData(payload) {
            MessageId   = Guid.NewGuid().ToString(),
            ContentType = "text/plain"
        };
        eventData.Properties["EventType"]  = "Raw.PartitionTest";
        eventData.Properties["StreamName"] = stream;

        using var batch = await producer.CreateBatchAsync(new CreateBatchOptions { PartitionKey = stream }, cancellationToken);
        if (!batch.TryAdd(eventData)) throw new InvalidOperationException("Failed to add event to batch");
        await producer.SendAsync(batch, cancellationToken);

        var received = (await Task.WhenAny(readers)).Result;
        await Assert.That(received).IsNotNull();
        await Assert.That(received!.MessageId).IsNotEmpty();
    }
}


