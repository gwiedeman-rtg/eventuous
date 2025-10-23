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
using Testcontainers.Azurite;
using Testcontainers.EventHubs;
using TUnit.Core.Interfaces;

namespace Eventuous.Tests.Azure.EventHubs.Integration;

/// <summary>
/// Test fixture for Azure Event Hubs integration tests
/// Uses Testcontainers for Azurite (Azure Storage Emulator) and Event Hubs emulator
/// Follows the same pattern as ServiceBus tests
/// </summary>
public class AzureEventHubsFixture : IAsyncInitializer, IAsyncDisposable {
    public EventHubProducerClient ProducerClient { get; private set; } = null!;
    public BlobServiceClient BlobServiceClient { get; private set; } = null!;
    public TableServiceClient TableServiceClient { get; private set; } = null!;
    public ServiceProvider ServiceProvider { get; private set; } = null!;
    public IEventStore EventStore { get; private set; } = null!;

    // Testcontainers
    public AzuriteContainer AzuriteContainer { get; } = new AzuriteBuilder()
        .WithImage("mcr.microsoft.com/azure-storage/azurite:latest")
        .Build();

    public EventHubsContainer EventHubsContainer { get; } = new EventHubsBuilder()
        .WithAcceptLicenseAgreement(true)
        .WithConfigurationBuilder(GetServiceConfiguration())
        .Build();

    // Connection strings
    public string BlobStorageConnectionString { get; private set; } = null!;
    public string TableStorageConnectionString { get; private set; } = null!;
    public string EventHubConnectionString { get; private set; } = null!;

    public async Task InitializeAsync() {
        // Start Azurite container for Azure Storage services
        await AzuriteContainer.StartAsync();

        // Start Event Hubs container
        await EventHubsContainer.StartAsync();

        // Get connection strings from containers
        // Use hardcoded connection strings for Azurite (following existing pattern)
        BlobStorageConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
        TableStorageConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";
        EventHubConnectionString = EventHubsContainer.GetConnectionString();

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
        await AzuriteContainer.DisposeAsync();
        await EventHubsContainer.DisposeAsync();
    }

    private static EventHubsServiceConfiguration GetServiceConfiguration() {
        return EventHubsServiceConfiguration.Create()
            .WithEntity("test-hub", 2, "$Default", "test-consumer-group");
    }
}

