// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs.Subscriptions;
using Eventuous.Tests.Azure.EventHubs.Integration.Subscriptions;
using TUnit.Assertions.AssertConditions.Throws;

// ReSharper disable UnusedType.Global

namespace Eventuous.Tests.Azure.EventHubs.Integration.Subscriptions;

/// <summary>
/// Basic subscription tests for Azure Event Hubs
/// TODO: Implement full subscription test base classes when Azure Event Hubs subscription types are available
/// </summary>
[NotInParallel]
public class BasicSubscriptionTests {
    [Test]
    public async Task Should_Create_Subscription_Options() {
        var options = new AzureEventHubsSubscriptionOptions {
            EventHubConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;",
            EventHubName = "test-hub",
            ConsumerGroup = "$Default",
            BlobStorageConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
            CheckpointContainerName = "test-container"
        };

        // Should not throw when validating
        options.Validate();

        await Assert.That(options.EventHubName).IsEqualTo("test-hub");
        await Assert.That(options.ConsumerGroup).IsEqualTo("$Default");
    }

    [Test]
    public async Task Should_Validate_Required_Options() {
        var options = new AzureEventHubsSubscriptionOptions();

        await Assert.That(() => options.Validate()).Throws<InvalidOperationException>();

        options.EventHubConnectionString = "test";
        await Assert.That(() => options.Validate()).Throws<InvalidOperationException>();

        options.EventHubName = "test";
        await Assert.That(() => options.Validate()).Throws<InvalidOperationException>();

        options.BlobStorageConnectionString = "test";
        // Should not throw now
        options.Validate();
    }
}