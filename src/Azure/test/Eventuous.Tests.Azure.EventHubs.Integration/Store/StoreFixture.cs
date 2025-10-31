// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using Eventuous.Azure.EventHubs.Extensions;
using Eventuous.Tests.Persistence.Base.Fixtures;
using Eventuous.Tests.Azure.EventHubs.Integration.Fixtures;
using Eventuous.TestHelpers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Testcontainers.EventHubs;
using DotNet.Testcontainers.Containers;
using Testcontainers.Azurite;
using DotNet.Testcontainers.Networks;

namespace Eventuous.Tests.Azure.EventHubs.Integration.Store;

/// <summary>
/// Test fixture for Azure Event Hubs integration tests using Docker containers
/// Uses Event Hubs Emulator with internal Azurite for blob/table storage
/// Follows the Postgres pattern with StoreFixtureBase<TContainer>
/// Can also use external Azure resources if environment variables are set
/// </summary>
public class StoreFixture : StoreFixtureBase<Testcontainers.EventHubs.EventHubsContainer>, IAsyncDisposable {
    public string EventHubConnectionString { get; private set; } = null!;
    public string BlobStorageConnectionString { get; private set; } = null!;
    public string TableStorageConnectionString { get; private set; } = null!;

    public AzuriteContainer? AzuriteContainer { get; private set; } = null;

    public INetwork? Network { get; private set; } = null;

    public StoreFixture() : base(LogLevel.Information) { }

    protected override void SetupServices(IServiceCollection services) {
        // Get connection string from container (base class provides Container property)
        EventHubConnectionString = Container.GetConnectionString();

        // NOTE: The Event Hubs emulator includes internal Azurite, but the ports may not be exposed.
        // For now, we use localhost endpoints assuming Azurite ports are mapped at the Docker level.
        // If these don't work, we may need to run a separate Azurite container or disable blob capture tests.

        BlobStorageConnectionString = $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:{AzuriteContainer!.GetMappedPublicPort(10000)}/devstoreaccount1;";
        TableStorageConnectionString = $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:{AzuriteContainer!.GetMappedPublicPort(10002)}/devstoreaccount1;";

        // Add Azure Event Hubs Event Store
        // Note: Atomic versioning is disabled by default to avoid requiring Table Storage for tests
        services.AddAzureEventHubsEventStore(options => {
            options.EventHubConnectionString = EventHubConnectionString;
            options.EventHubName = "test-hub";
            options.BlobStorageConnectionString = BlobStorageConnectionString;
            options.CaptureContainerName = "test-container";
            options.TableStorageConnectionString = TableStorageConnectionString;
            options.ConsumerGroup = "$Default";
            options.UseRealtimeReading = true;
            options.EnableAtomicVersioning = false; // Disabled by default - enable when testing atomic versioning
        });

        // Register the EventStore service - base class will automatically set EventStore property
        //services.AddEventStore<AzureEventHubsEventStore>();
    }

    protected override Testcontainers.EventHubs.EventHubsContainer CreateContainer()
    {
        // Check if external resources are configured via environment variables
        // If they are, throw immediately before attempting Docker operations
        var hasEventHubsEnv = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("EVENTHUBS_CONNECTION_STRING"));
        var hasBlobEnv = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("BLOB_STORAGE_CONNECTION_STRING"));

        if (hasEventHubsEnv && hasBlobEnv) {
            throw new InvalidOperationException(
                "External Azure resources are configured via environment variables (EVENTHUBS_CONNECTION_STRING and BLOB_STORAGE_CONNECTION_STRING). " +
                "StoreFixture requires Docker containers. Please use ExternalBlobLeaseVersionStrategyFixture or ExternalTableStorageVersionStrategyFixture instead, " +
                "or unset the environment variables to use Docker containers with StoreFixture."
            );
        }

        try {
            Network = EventHubsContainerBuilder.CreateNetwork();

            AzuriteContainer = EventHubsContainerBuilder.CreateAzurite().WithNetwork(Network).WithNetworkAliases("evhub").Build();

            return EventHubsContainerBuilder.CreateBuilder().WithAzuriteContainer(Network, AzuriteContainer, "evhub").Build();
        }
        catch (Exception ex) when (ex.GetType().Name.Contains("Docker") || ex.Message.Contains("Docker", StringComparison.OrdinalIgnoreCase) || ex.Message.Contains("docker", StringComparison.OrdinalIgnoreCase)) {
            throw new InvalidOperationException(
                "Docker is not available. Please ensure Docker is running, or set EVENTHUBS_CONNECTION_STRING and BLOB_STORAGE_CONNECTION_STRING " +
                "environment variables and use ExternalBlobLeaseVersionStrategyFixture or ExternalTableStorageVersionStrategyFixture.",
                ex
            );
        }
    }

    public override async ValueTask DisposeAsync() {
        if (AzuriteContainer != null)
            await AzuriteContainer.DisposeAsync();

        if (Network != null)
            await Network.DisposeAsync();

        await base.DisposeAsync();
    }

}