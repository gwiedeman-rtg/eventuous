// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using Eventuous.Azure.EventHubs.Extensions;
using Eventuous.Tests.Persistence.Base.Fixtures;
using Eventuous.Tests.Azure.EventHubs.Integration.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.EventHubs;

namespace Eventuous.Tests.Azure.EventHubs.Integration.Store;

/// <summary>
/// Test fixture for Azure Event Hubs integration tests using Docker containers
/// Uses Event Hubs Emulator with internal Azurite for blob/table storage
/// Follows the Postgres pattern with StoreFixtureBase<TContainer>
/// </summary>
public class StoreFixture : StoreFixtureBase<Testcontainers.EventHubs.EventHubsContainer> {
    public string EventHubConnectionString { get; private set; } = null!;
    public string BlobStorageConnectionString { get; private set; } = null!;
    public string TableStorageConnectionString { get; private set; } = null!;

    public StoreFixture() : base(LogLevel.Information) { }

    protected override void SetupServices(IServiceCollection services) {
        // Get connection string from container (base class provides Container property)
        EventHubConnectionString = Container.GetConnectionString();
        
        // Event Hubs emulator includes internal Azurite, so we use the same connection strings
        // The emulator provides blob and table storage endpoints
        BlobStorageConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
        TableStorageConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";

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
        
        // Register the EventStore service - base class will automatically set EventStore property
        services.AddEventStore<AzureEventHubsEventStore>();
    }

    protected override Testcontainers.EventHubs.EventHubsContainer CreateContainer() => 
        EventHubsContainerBuilder.CreateBuilder().Build();
}