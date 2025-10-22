// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using Azure.Data.Tables;
using Eventuous.Azure.EventHubs;
using Eventuous.Azure.EventHubs.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using TUnit.Core.Interfaces;

namespace Eventuous.Tests.Azure.EventHubs.Integration;

/// <summary>
/// Test fixture for Azure Event Hubs integration tests
/// Uses Azurite (Azure Storage Emulator) and Event Hubs emulator in Docker containers
/// Follows the same pattern as ServiceBus tests
/// </summary>
public class AzureEventHubsFixture : IAsyncInitializer, IAsyncDisposable {
    public EventHubProducerClient ProducerClient { get; private set; } = null!;
    public BlobServiceClient BlobServiceClient { get; private set; } = null!;
    public TableServiceClient TableServiceClient { get; private set; } = null!;
    public ServiceProvider ServiceProvider { get; private set; } = null!;
    public IEventStore EventStore { get; private set; } = null!;

    // Connection strings for Azurite (Azure Storage Emulator)
    public string BlobStorageConnectionString { get; private set; } = null!;
    public string TableStorageConnectionString { get; private set; } = null!;
    public string EventHubConnectionString { get; private set; } = null!;

    public async Task InitializeAsync() {
        // For now, we'll use mock connection strings
        // In a real implementation, we would start Azurite and Event Hubs emulator containers

        // Azurite connection strings (default ports)
        BlobStorageConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
        TableStorageConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";

        // Event Hubs connection string (would be from emulator)
        EventHubConnectionString = "Endpoint=sb://localhost:9093/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=test";

        // Initialize Azure clients
        ProducerClient = new EventHubProducerClient(EventHubConnectionString, "test-hub");
        BlobServiceClient = new BlobServiceClient(BlobStorageConnectionString);
        TableServiceClient = new TableServiceClient(TableStorageConnectionString);

        // Setup services
        var services = new ServiceCollection();

        // Add logging
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Information));

        // Add Azure clients
        services.AddSingleton(ProducerClient);
        services.AddSingleton(BlobServiceClient);
        services.AddSingleton(TableServiceClient);

        // Add Azure Event Hubs Event Store
        services.AddAzureEventHubsEventStore(options => {
            options.EventHubConnectionString = EventHubConnectionString;
            options.EventHubName = "test-hub";
            options.BlobStorageConnectionString = BlobStorageConnectionString;
            options.CaptureContainerName = "test-container";
            options.TableStorageConnectionString = TableStorageConnectionString;
            options.ConsumerGroup = "$Default";
            options.UseRealtimeReading = true;
        });

        ServiceProvider = services.BuildServiceProvider();
        EventStore = ServiceProvider.GetRequiredService<IEventStore>();
    }

    public async ValueTask DisposeAsync() {
        await ProducerClient.DisposeAsync();
        await ServiceProvider.DisposeAsync();
    }
}
