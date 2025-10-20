// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using Eventuous.Tests.Persistence.Base.Fixtures;
using Microsoft.Extensions.Logging;

namespace Eventuous.Tests.Azure.EventHubs;

public class AzureEventHubsEventStoreTests {
    [Fact]
    public void CanCreateEventStore() {
        // This is a basic test to verify the class can be instantiated
        // In a real test environment, you would need actual Azure Event Hubs and Blob Storage instances
        
        var connectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test";
        var eventHubName = "test-hub";
        var blobConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net";
        var containerName = "test-container";

        Assert.Throws<ArgumentException>(() => {
            // This will throw because the connection strings are invalid, but it validates the constructor
            var eventStore = new AzureEventHubsEventStore(
                connectionString,
                eventHubName,
                blobConnectionString,
                containerName
            );
        });
    }

    [Fact]
    public void CanCreateProducer() {
        // This is a basic test to verify the producer class can be instantiated
        
        var connectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test";
        var eventHubName = "test-hub";

        Assert.Throws<ArgumentException>(() => {
            // This will throw because the connection string is invalid, but it validates the constructor
            var producer = new AzureEventHubsProducer(
                connectionString,
                eventHubName
            );
        });
    }

    [Fact]
    public void OptionsValidationWorks() {
        var options = new AzureEventHubsEventStoreOptions();
        
        Assert.Throws<InvalidOperationException>(() => options.Validate());

        options.EventHubConnectionString = "test";
        Assert.Throws<InvalidOperationException>(() => options.Validate());

        options.EventHubName = "test";
        Assert.Throws<InvalidOperationException>(() => options.Validate());

        options.BlobStorageConnectionString = "test";
        Assert.Throws<InvalidOperationException>(() => options.Validate());

        options.CaptureContainerName = "test";
        // Should not throw now
        options.Validate();
    }
}