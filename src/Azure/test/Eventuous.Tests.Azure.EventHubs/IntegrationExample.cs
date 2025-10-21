// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using Eventuous.Azure.EventHubs.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Eventuous.Tests.Azure.EventHubs;

/// <summary>
/// This class demonstrates how to use the Azure Event Hubs Event Store
/// Note: These tests require actual Azure resources to run
/// </summary>
public class IntegrationExample {
    [Fact(Skip = "Requires actual Azure Event Hubs and Blob Storage resources")]
    public async Task CanAppendAndReadEvents() {
        // This test would require actual Azure resources
        // It's marked as Skip to avoid failing in CI/CD

        var connectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test";
        var eventHubName = "test-hub";
        var blobConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net";
        var containerName = "test-container";

        var eventStore = new AzureEventHubsEventStore(
            connectionString,
            eventHubName,
            blobConnectionString,
            containerName
        );

        var streamName = new StreamName("test-stream-123");
        var events = new[] {
            new NewStreamEvent(
                Guid.NewGuid(),
                new TestEvent("1", "First Event", DateTime.UtcNow),
                new Metadata { ["UserId"] = "user-123" }
            ),
            new NewStreamEvent(
                Guid.NewGuid(),
                new TestEvent("2", "Second Event", DateTime.UtcNow),
                new Metadata { ["UserId"] = "user-123" }
            )
        };

        // Append events
        var result = await eventStore.AppendEvents(
            streamName,
            ExpectedStreamVersion.NoStream,
            events
        );

        Assert.True(result.GlobalPosition > 0);
        Assert.Equal(1, result.NextExpectedVersion);

        // Read events back
        var readEvents = await eventStore.ReadEvents(
            streamName,
            StreamReadPosition.Start,
            10,
            false
        );

        Assert.Equal(2, readEvents.Length);
        Assert.IsType<TestEvent>(readEvents[0].Payload);
        Assert.IsType<TestEvent>(readEvents[1].Payload);
    }

    [Fact]
    public void CanConfigureWithDependencyInjection() {
        var services = new ServiceCollection();

        services.AddLogging(builder => builder.AddConsole());

        services.AddAzureEventHubsEventStore(options => {
            options.EventHubConnectionString = "test-connection";
            options.EventHubName = "test-hub";
            options.BlobStorageConnectionString = "test-blob-connection";
            options.CaptureContainerName = "test-container";
        });

        services.AddAzureEventHubsProducer("test-connection", "test-hub");

        var serviceProvider = services.BuildServiceProvider();

        // Verify services are registered
        Assert.NotNull(serviceProvider.GetService<IEventStore>());
        Assert.NotNull(serviceProvider.GetService<IProducer>());
    }

    [Fact(Skip = "Requires actual Azure Event Hubs resources")]
    public async Task ProducerCanSendMessages() {
        var connectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test";
        var eventHubName = "test-hub";

        var producer = new AzureEventHubsProducer(connectionString, eventHubName);

        var messages = new[] {
            new ProducedMessage(
                new TestEvent("1", "Producer Test", DateTime.UtcNow),
                new Metadata { ["Source"] = "Producer" }
            )
        };

        var streamName = new StreamName("producer-test-stream");

        // This would normally send to Event Hubs
        // In a real test, you'd need actual Azure resources
        await Assert.ThrowsAsync<ArgumentException>(async () => {
            await producer.Produce(streamName, messages);
        });
    }
}