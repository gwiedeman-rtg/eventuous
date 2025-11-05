// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using TUnit.Core;
using TUnit.Assertions;
using Eventuous.Tests.Azure.EventHubs.Integration.Store;
using Azure.Messaging.EventHubs.Consumer;
using System.Threading.Tasks;

namespace Eventuous.Tests.Azure.EventHubs.Integration;

/// <summary>
/// Test to manually verify Event Hubs is working
/// </summary>
[ClassDataSource<StoreFixture>]
public class ManualEventHubTest {
    readonly StoreFixture _fixture;

    public ManualEventHubTest(StoreFixture fixture) {
        _fixture = fixture;
    }

    [Test]
    public async Task SendAndReceiveEvent() {
        // Get a connection string and hub name from the fixture
        var connectionString = _fixture.EventHubConnectionString;
        var eventHubName = "test-hub";

        System.Diagnostics.Debug.WriteLine($"Connection String: {connectionString}");

        // Create a producer
        await using var producer = new EventHubProducerClient(connectionString, eventHubName);

        // Send a test event directly
        var testEvent = new EventData(System.Text.Encoding.UTF8.GetBytes("Hello from manual test")) {
            MessageId = Guid.NewGuid().ToString(),
            ContentType = "text/plain"
        };
        testEvent.Properties["EventType"] = "TestEvent";
        testEvent.Properties["StreamName"] = "test-stream";
        testEvent.Properties["TestProperty"] = "TestValue";

        await producer.SendAsync(new[] { testEvent });

        System.Diagnostics.Debug.WriteLine($"Sent event with MessageId={testEvent.MessageId}");

        // Try to read it back
        await using var consumer = new EventHubConsumerClient(
            "$Default",
            connectionString,
            eventHubName
        );

        var partitionIds = await consumer.GetPartitionIdsAsync();
        System.Diagnostics.Debug.WriteLine($"Found {partitionIds.Length} partitions");

        var eventsRead = 0;
        var readOptions = new ReadEventOptions { MaximumWaitTime = TimeSpan.FromSeconds(5) };

        foreach (var partitionId in partitionIds) {
            System.Diagnostics.Debug.WriteLine($"Reading from partition {partitionId}");

            await foreach (var partitionEvent in consumer.ReadEventsFromPartitionAsync(
                partitionId,
                EventPosition.Earliest,
                readOptions
            )) {
                eventsRead++;
                System.Diagnostics.Debug.WriteLine($"Read event {eventsRead}: Data={partitionEvent.Data}, MessageId={partitionEvent.Data?.MessageId}");

                if (eventsRead > 10) break;
            }

            if (eventsRead > 0) break;
        }

        // Manual verification: eventsRead should be > 0
        Console.WriteLine($"Events read: {eventsRead}");
    }
}

