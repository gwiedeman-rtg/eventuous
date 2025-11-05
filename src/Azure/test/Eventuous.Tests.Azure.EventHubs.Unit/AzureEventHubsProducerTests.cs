// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using TUnit.Assertions.AssertConditions.Throws;

namespace Eventuous.Tests.Azure.EventHubs.Unit;

/// <summary>
/// Unit tests for AzureEventHubsProducer
/// </summary>
public class AzureEventHubsProducerTests {

    [Test]
    public async Task Should_Throw_On_Invalid_ConnectionString() {
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

    [Test]
    public async Task Should_Throw_On_Null_EventHubName() {
        await Assert.That(() => {
            var producer = new AzureEventHubsProducer("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test", null!);
        }).Throws<ArgumentException>();
    }
}
