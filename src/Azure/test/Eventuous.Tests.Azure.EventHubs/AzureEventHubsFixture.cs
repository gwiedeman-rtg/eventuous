// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Eventuous.Tests.Azure.EventHubs;

/// <summary>
/// Test fixture for Azure Event Hubs integration tests
/// Requires actual Azure Event Hubs and Blob Storage resources
/// </summary>
public class AzureEventHubsFixture : IDisposable {
    public AzureEventHubsEventStore EventStore { get; }
    public AzureEventHubsProducer Producer { get; }
    public ILogger<AzureEventHubsFixture> Logger { get; }
    
    public string EventHubConnectionString { get; }
    public string EventHubName { get; }
    public string BlobStorageConnectionString { get; }
    public string CaptureContainerName { get; }

    public AzureEventHubsFixture() {
        // Load configuration from environment variables or appsettings.json
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.test.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        EventHubConnectionString = configuration["Azure:EventHubs:ConnectionString"] 
            ?? throw new InvalidOperationException("Azure:EventHubs:ConnectionString is required for integration tests");
        
        EventHubName = configuration["Azure:EventHubs:EventHubName"] 
            ?? "eventuous-test-hub";
        
        BlobStorageConnectionString = configuration["Azure:BlobStorage:ConnectionString"] 
            ?? throw new InvalidOperationException("Azure:BlobStorage:ConnectionString is required for integration tests");
        
        CaptureContainerName = configuration["Azure:BlobStorage:CaptureContainer"] 
            ?? "eventhubs-capture";

        var loggerFactory = LoggerFactory.Create(builder => 
            builder.AddConsole().SetMinimumLevel(LogLevel.Debug));
        
        Logger = loggerFactory.CreateLogger<AzureEventHubsFixture>();

        EventStore = new AzureEventHubsEventStore(
            EventHubConnectionString,
            EventHubName,
            BlobStorageConnectionString,
            CaptureContainerName,
            consumerGroup: "$Default",
            useRealtimeReading: true,
            logger: loggerFactory.CreateLogger<AzureEventHubsEventStore>()
        );

        Producer = new AzureEventHubsProducer(
            EventHubConnectionString,
            EventHubName,
            logger: loggerFactory.CreateLogger<AzureEventHubsProducer>()
        );
    }

    public void Dispose() {
        EventStore.Dispose();
        Producer.Dispose();
    }
}

/// <summary>
/// Collection definition for Azure Event Hubs tests
/// </summary>
[CollectionDefinition("AzureEventHubs")]
public class AzureEventHubsCollection : ICollectionFixture<AzureEventHubsFixture> {
}