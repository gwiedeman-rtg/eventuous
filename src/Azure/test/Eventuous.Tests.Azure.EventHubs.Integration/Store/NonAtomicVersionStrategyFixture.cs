// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using Eventuous.Azure.EventHubs.Extensions;
using Eventuous.Tests.Persistence.Base.Fixtures;
using Eventuous.Tests.Azure.EventHubs.Integration.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.EventHubs;
using Testcontainers.Azurite;
using DotNet.Testcontainers.Networks;

namespace Eventuous.Tests.Azure.EventHubs.Integration.Store;

/// <summary>
/// Test fixture for Azure Event Hubs integration tests using NonAtomicVersionStrategy
///
/// This fixture tests the NonAtomicVersionStrategy which:
/// - Uses GetCurrentStreamVersion() to determine stream versions
/// - Is fastest but subject to race conditions
/// - Relies on reading events from Event Hubs and blob storage
/// - May skip version validation if version cannot be determined
///
/// Use this fixture to test scenarios where:
/// - Performance is more important than strict consistency
/// - Race conditions are acceptable
/// - Simple deployment without additional Azure services
/// </summary>
public class NonAtomicVersionStrategyFixture : StoreFixtureBase<Testcontainers.EventHubs.EventHubsContainer> {
    public string EventHubConnectionString { get; private set; } = null!;
    public string BlobStorageConnectionString { get; private set; } = null!;
    public string TableStorageConnectionString { get; private set; } = null!;

    public AzuriteContainer? AzuriteContainer { get; private set; } = null;
    public INetwork? Network { get; private set; } = null;

    public NonAtomicVersionStrategyFixture() : base(LogLevel.Information) { }

    protected override void SetupServices(IServiceCollection services) {
        // Get connection string from container (base class provides Container property)
        EventHubConnectionString = Container.GetConnectionString();

        // Configure Azurite for blob storage (Event Hubs Capture) and table storage
        BlobStorageConnectionString = $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:{AzuriteContainer?.GetMappedPublicPort(10000) ?? 10000}/devstoreaccount1;";
        TableStorageConnectionString = $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:{AzuriteContainer?.GetMappedPublicPort(10002) ?? 10002}/devstoreaccount1;";

        // Configure Azure Event Hubs Event Store with NonAtomicVersionStrategy
        services.AddAzureEventHubsEventStore(options => {
            options.EventHubConnectionString = EventHubConnectionString;
            options.EventHubName = "test-hub";
            options.BlobStorageConnectionString = BlobStorageConnectionString;
            options.CaptureContainerName = "test-container";
            options.TableStorageConnectionString = TableStorageConnectionString;
            options.ConsumerGroup = "$Default";
            options.UseRealtimeReading = true;

            // CRITICAL: Disable atomic versioning to use NonAtomicVersionStrategy
            // This strategy uses GetCurrentStreamVersion() which may be unreliable
            // due to Event Hubs consumer timing issues and blob storage delays
            options.EnableAtomicVersioning = false;
        });

        // Register the EventStore service - base class will automatically set EventStore property
        services.AddEventStore<AzureEventHubsEventStore>();
    }

    protected override Testcontainers.EventHubs.EventHubsContainer CreateContainer() {
        // Create network for Event Hubs and Azurite containers
        Network = EventHubsContainerBuilder.CreateNetwork();

        // Create Azurite container for blob and table storage
        AzuriteContainer = new AzuriteBuilder()
            .WithNetwork(Network)
            .WithPortBinding(10000, 10000) // Blob service
            .WithPortBinding(10001, 10001) // Queue service
            .WithPortBinding(10002, 10002) // Table service
            .Build();

        // Create Event Hubs container
        return EventHubsContainerBuilder.CreateBuilder()
            .WithNetwork(Network)
            .WithPortBinding(9093, 9093)
            .Build();
    }

    public override async Task InitializeAsync() {
        // Start Azurite first
        await AzuriteContainer!.StartAsync();

        // Start Event Hubs
        await base.InitializeAsync();
    }

    public override async ValueTask DisposeAsync() {
        await base.DisposeAsync();
        if (AzuriteContainer != null) {
            await AzuriteContainer.DisposeAsync();
        }
        // Note: INetwork doesn't implement IDisposable, so we can't dispose it
        // The network will be cleaned up when the containers are disposed
    }
}
