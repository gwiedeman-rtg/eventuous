// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using TUnit.Assertions.AssertConditions.Throws;

namespace Eventuous.Tests.Azure.EventHubs.Unit;

/// <summary>
/// Unit tests for AzureEventHubsEventStoreOptions
/// </summary>
public class AzureEventHubsEventStoreOptionsTests {

    [Test]
    public async Task Should_Require_EventHubConnectionString() {
        var options = new AzureEventHubsEventStoreOptions();

        await Assert.That(() => options.Validate()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Should_Require_EventHubName() {
        var options = new AzureEventHubsEventStoreOptions {
            EventHubConnectionString = "test"
        };

        await Assert.That(() => options.Validate()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Should_Require_BlobStorageConnectionString() {
        var options = new AzureEventHubsEventStoreOptions {
            EventHubConnectionString = "test",
            EventHubName = "test"
        };

        await Assert.That(() => options.Validate()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Should_Require_CaptureContainerName() {
        var options = new AzureEventHubsEventStoreOptions {
            EventHubConnectionString = "test",
            EventHubName = "test",
            BlobStorageConnectionString = "test"
        };

        await Assert.That(() => options.Validate()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Should_Require_TableStorageConnectionString() {
        var options = new AzureEventHubsEventStoreOptions {
            EventHubConnectionString = "test",
            EventHubName = "test",
            BlobStorageConnectionString = "test",
            CaptureContainerName = "test"
        };

        await Assert.That(() => options.Validate()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Should_Validate_When_All_Required_Properties_Set() {
        var options = new AzureEventHubsEventStoreOptions {
            EventHubConnectionString = "test",
            EventHubName = "test",
            BlobStorageConnectionString = "test",
            CaptureContainerName = "test",
            TableStorageConnectionString = "test"
        };

        // Should not throw
        options.Validate();
    }

    [Test]
    public async Task Should_Have_Default_Values() {
        var options = new AzureEventHubsEventStoreOptions();

        await Assert.That(options.ConsumerGroup).IsEqualTo("$Default");
        await Assert.That(options.UseRealtimeReading).IsEqualTo(true);
    }
}
