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
/// Test fixture for Azure Event Hubs integration tests using BlobLeaseVersionStrategy
///
/// This fixture tests the BlobLeaseVersionStrategy which:
/// - Uses Azure Blob Lease for distributed locking during version checks
/// - Provides strong consistency guarantees without requiring Table Storage
/// - Uses blob leases to prevent concurrent modifications
/// - Requires Azure Blob Storage service
///
/// Use this fixture to test scenarios where:
/// - Strong consistency is required
/// - Azure Table Storage is not available
/// - Azure Blob Storage is available
/// - Distributed locking via blob leases is acceptable
/// </summary>
public class BlobLeaseVersionStrategyFixture : StoreFixtureBase<Testcontainers.EventHubs.EventHubsContainer> {
    public string EventHubConnectionString { get; private set; } = null!;
    public string BlobStorageConnectionString { get; private set; } = null!;
    public string TableStorageConnectionString { get; private set; } = null!;

    public AzuriteContainer? AzuriteContainer { get; private set; } = null;
    public INetwork? Network { get; private set; } = null;

    public BlobLeaseVersionStrategyFixture() : base(LogLevel.Information) { }

    protected override void SetupServices(IServiceCollection services) {
        // Get connection string from container (base class provides Container property)
        EventHubConnectionString = Container.GetConnectionString();

        // Configure Azurite for blob storage (Event Hubs Capture) and table storage
        BlobStorageConnectionString = $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:{AzuriteContainer?.GetMappedPublicPort(10000) ?? 10000}/devstoreaccount1;";
        TableStorageConnectionString = $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:{AzuriteContainer?.GetMappedPublicPort(10002) ?? 10002}/devstoreaccount1;";

        // Configure Azure Event Hubs Event Store with BlobLeaseVersionStrategy
        services.AddAzureEventHubsEventStore(options => {
            options.EventHubConnectionString = EventHubConnectionString;
            options.EventHubName = "test-hub";
            options.BlobStorageConnectionString = BlobStorageConnectionString;
            options.CaptureContainerName = "test-container";
            options.TableStorageConnectionString = null; // CRITICAL: Set to null to force BlobLeaseVersionStrategy
            options.ConsumerGroup = "$Default";
            options.UseRealtimeReading = true;

            // CRITICAL: Enable atomic versioning without Table Storage
            // This forces the use of BlobLeaseVersionStrategy which uses blob leases
            // for distributed locking during version checks
            options.EnableAtomicVersioning = true;
        });

        // Register the EventStore service - base class will automatically set EventStore property
        services.AddEventStore<AzureEventHubsEventStore>();
    }

    protected override Testcontainers.EventHubs.EventHubsContainer CreateContainer() {
        // Create network for Event Hubs and Azurite containers
        Network = EventHubsContainerBuilder.CreateNetwork();

        // Create Azurite container for blob storage
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
