// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.CosmosDb;
using Eventuous.Azure.CosmosDb.Extensions;
using Eventuous.Tests.Persistence.Base.Fixtures;
using Eventuous.Tests.Azure.CosmosDb.Integration.Fixtures;
using Eventuous.TestHelpers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;
using Testcontainers.CosmosDb;

namespace Eventuous.Tests.Azure.CosmosDb.Integration.Store;

/// <summary>
/// Test fixture for Azure Cosmos DB integration tests using Docker containers
/// Uses Cosmos DB Emulator container with official Testcontainers.CosmosDb package
/// </summary>
public class StoreFixture : StoreFixtureBase<CosmosDbContainer>, IAsyncDisposable {
    public string ConnectionString { get; private set; } = null!;
    public string AccountEndpoint { get; private set; } = null!;
    public string AccountKey { get; private set; } = null!;

    public StoreFixture() : base(LogLevel.Information) { }

    public override async Task InitializeAsync() {
        // Call base initialization which creates and starts the container
        await base.InitializeAsync();

        // Give the Cosmos DB emulator additional time to fully initialize
        // Even after it logs "Started", it may need more time for the HTTP/HTTPS endpoints to be ready
        // According to Microsoft docs: https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-develop-emulator
        // The emulator needs time to fully start up before accepting connections
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Manually ensure database and container are created now that emulator is ready
        // The initialization in SetupServices might have failed if emulator wasn't ready
        var cosmosClient = Provider.GetRequiredService<CosmosClient>();
        var options = Provider.GetRequiredService<CosmosDbEventStoreOptions>();
        
        try {
            var databaseResponse = await cosmosClient.CreateDatabaseIfNotExistsAsync(options.Database);
            var containerProperties = new ContainerProperties {
                Id = options.Container,
                PartitionKeyPath = options.PartitionKeyPath ?? "/streamId"
            };
            await databaseResponse.Database.CreateContainerIfNotExistsAsync(containerProperties);
        } catch (Exception ex) {
            throw new InvalidOperationException(
                $"Failed to create Cosmos DB resources after delay. Database: '{options.Database}', Container: '{options.Container}'. " +
                "The emulator may not be fully ready yet.",
                ex
            );
        }
    }

    protected override void SetupServices(IServiceCollection services) {
        // Use the official Testcontainers.CosmosDb connection string
        // This container has built-in support for the emulator's quirks
        ConnectionString = Container.GetConnectionString();
        
        // The container provides an HttpClient that handles SSL certificate issues
        // We keep a reference to it without disposing (the container manages its lifecycle)
        var httpClient = Container.HttpClient;

        // Create CosmosClient with the container's pre-configured HttpClient
        var cosmosClientOptions = new CosmosClientOptions {
            ConnectionMode = ConnectionMode.Gateway,
            HttpClientFactory = () => httpClient
        };

        var cosmosClient = new CosmosClient(ConnectionString, cosmosClientOptions);

        // Register the CosmosClient and use the overload that accepts an existing client
        services.AddCosmosDbEventStore(
            cosmosClient,
            "eventstore-test",
            "events",
            "/streamId"
        );

        // EventStore is already registered by AddCosmosDbEventStore, base class will get it automatically
    }

    protected override CosmosDbContainer CreateContainer() {
        try {
            return CosmosDbContainerBuilder.Create();
        } catch (Exception ex) when (ex.GetType().Name.Contains("Docker") || ex.Message.Contains("Docker", StringComparison.OrdinalIgnoreCase) || ex.Message.Contains("docker", StringComparison.OrdinalIgnoreCase)) {
            throw new InvalidOperationException(
                "Docker is not available. Please ensure Docker is running to run integration tests.",
                ex
            );
        }
    }
}

