// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using TUnit.Assertions.AssertConditions.Throws;

namespace Eventuous.Tests.Azure.EventHubs.Unit;

/// <summary>
/// Basic unit tests for Azure Event Hubs components
/// </summary>
public class BasicUnitTests {

    [Test]
    public async Task Should_Validate_EventStore_Options_Requires_All_Properties() {
        var options = new AzureEventHubsEventStoreOptions();

        // Should throw when missing required properties
        await Assert.That(() => options.Validate()).Throws<InvalidOperationException>();

        // Set required properties one by one
        options.EventHubConnectionString = "test";
        await Assert.That(() => options.Validate()).Throws<InvalidOperationException>();

        options.EventHubName = "test";
        await Assert.That(() => options.Validate()).Throws<InvalidOperationException>();

        options.BlobStorageConnectionString = "test";
        await Assert.That(() => options.Validate()).Throws<InvalidOperationException>();

        options.CaptureContainerName = "test";
        await Assert.That(() => options.Validate()).Throws<InvalidOperationException>();

        options.TableStorageConnectionString = "test";

        // Should not throw when all properties are set
        options.Validate();
    }

    [Test]
    public async Task Should_Have_Default_Values() {
        var options = new AzureEventHubsEventStoreOptions();

        await Assert.That(options.ConsumerGroup).IsEqualTo("$Default");
        await Assert.That(options.UseRealtimeReading).IsEqualTo(true);
    }

    [Test]
    public async Task Should_Throw_On_Invalid_Producer_Connection() {
        await Assert.That(() => {
            var producer = new AzureEventHubsProducer("invalid-connection", "test-hub");
        }).Throws<FormatException>();
    }

    [Test]
    public async Task Should_Throw_On_Empty_EventHubName() {
        await Assert.That(() => {
            var producer = new AzureEventHubsProducer("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test", "");
        }).Throws<ArgumentException>();
    }
}
