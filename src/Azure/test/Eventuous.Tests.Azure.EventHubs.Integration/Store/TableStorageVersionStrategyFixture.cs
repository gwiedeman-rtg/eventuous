// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using Eventuous.Azure.EventHubs.Extensions;
using Eventuous.Azure.EventHubs.Versioning;
using Eventuous.Tests.Persistence.Base.Fixtures;
using Eventuous.Tests.Azure.EventHubs.Integration.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Testcontainers.EventHubs;
using DotNet.Testcontainers.Containers;
using Testcontainers.Azurite;
using DotNet.Testcontainers.Networks;
using Azure.Data.Tables;

namespace Eventuous.Tests.Azure.EventHubs.Integration.Store;

/// <summary>
/// Test fixture for Azure Event Hubs integration tests using TableStorageVersionStrategy
/// Uses Event Hubs Emulator with internal Azurite for blob/table storage
/// Follows the Postgres pattern with StoreFixtureBase<TContainer>
/// </summary>
public class TableStorageVersionStrategyFixture : StoreFixtureBase<Testcontainers.EventHubs.EventHubsContainer> {
    public string EventHubConnectionString { get; private set; } = null!;
    public string BlobStorageConnectionString { get; private set; } = null!;
    public string TableStorageConnectionString { get; private set; } = null!;

    public AzuriteContainer? AzuriteContainer { get; private set; } = null;

    public INetwork? Network { get; private set; } = null;

    public TableStorageVersionStrategyFixture() : base(LogLevel.Information) { }

    protected override void SetupServices(IServiceCollection services) {
        // Get connection string from container (base class provides Container property)
        EventHubConnectionString = Container.GetConnectionString();

        // Use Azurite container for Blob Storage and Table Storage
        // The Azurite container provides the storage services needed for TableStorageVersionStrategy
        BlobStorageConnectionString = $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:{AzuriteContainer.GetMappedPublicPort(10000)}/devstoreaccount1;";
        TableStorageConnectionString = $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:{AzuriteContainer.GetMappedPublicPort(10002)}/devstoreaccount1;";

        // Register TableStorageVersionStrategy explicitly using the injectable approach
        services.AddSingleton<IStreamVersionStrategy>(serviceProvider => {
            var tableServiceClient = new TableServiceClient(TableStorageConnectionString);
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            return new TableStorageVersionStrategy(
                tableServiceClient,
                loggerFactory.CreateLogger<TableStorageVersionStrategy>()
            );
        });

        // Add Azure Event Hubs Event Store - will use the injected TableStorageVersionStrategy
        services.AddAzureEventHubsEventStore(options => {
            options.EventHubConnectionString = EventHubConnectionString;
            options.EventHubName = "test-hub";
            options.BlobStorageConnectionString = BlobStorageConnectionString;
            options.CaptureContainerName = "test-container";
            options.ConsumerGroup = "$Default";
            options.UseRealtimeReading = true;
            // Note: EnableAtomicVersioning is not needed when strategy is explicitly injected
        });

        // Register the EventStore service - base class will automatically set EventStore property
        //services.AddEventStore<AzureEventHubsEventStore>();
    }

    protected override Testcontainers.EventHubs.EventHubsContainer CreateContainer()
    {
        // Use shared network to avoid Docker network pool exhaustion
        Network = EventHubsContainerBuilder.CreateNetwork();

        // Create Azurite container with proper network configuration
        AzuriteContainer = EventHubsContainerBuilder.CreateAzurite()
            .WithNetwork(Network)
            .WithNetworkAliases("azurite")
            .Build();

        // Create EventHubs container with Azurite integration
        return EventHubsContainerBuilder.CreateBuilder()
            .WithAzuriteContainer(Network, AzuriteContainer, "azurite")
            .Build();
    }

    public override async ValueTask DisposeAsync() {
        if (AzuriteContainer != null)
            await AzuriteContainer.DisposeAsync();

        if (Network != null)
            await Network.DisposeAsync();

        await base.DisposeAsync();
    }

}