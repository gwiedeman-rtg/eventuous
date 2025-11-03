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
using DotNet.Testcontainers.Containers;

namespace Eventuous.Tests.Azure.CosmosDb.Integration.Store;

/// <summary>
/// Test fixture for Azure Cosmos DB integration tests using Docker containers
/// Uses Cosmos DB Emulator container
/// Follows the Postgres pattern with StoreFixtureBase<TContainer>
/// </summary>
public class StoreFixture : StoreFixtureBase<DockerContainer>, IAsyncDisposable {
    public string ConnectionString { get; private set; } = null!;
    public string AccountEndpoint { get; private set; } = null!;
    public string AccountKey { get; private set; } = null!;

    public StoreFixture() : base(LogLevel.Information) { }

    public override async Task InitializeAsync() {
        // Call base initialization which creates and starts the container
        await base.InitializeAsync();

        // Give the Cosmos DB emulator additional time to fully initialize
        // Even after it logs "Started", it may need more time for the HTTP/HTTPS endpoints to be ready
        // This is especially true when starting multiple partitions (10 in our case)
        await Task.Delay(TimeSpan.FromSeconds(5));
    }

    protected override void SetupServices(IServiceCollection services) {
        // Get connection details from container
        // Cosmos DB Emulator uses HTTPS on port 8081
        // Note: InitializeAsync adds a delay after container start to ensure port mappings are available
        var port = Container.GetMappedPublicPort(8081);

        // Use localhost instead of container hostname to ensure SSL validation bypass works
        // The port is mapped to localhost, so we connect via localhost
        // This ensures the SSL validation bypass in ServiceCollectionExtensions is triggered
        AccountEndpoint = $"https://localhost:{port}";
        AccountKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="; // Cosmos DB Emulator default key
        ConnectionString = $"AccountEndpoint={AccountEndpoint};AccountKey={AccountKey};";

        // Create CosmosClient with custom HTTP handler for the emulator
        // The emulator returns internal container IPs that need to be redirected to localhost
        var cosmosClientOptions = new CosmosClientOptions {
            ConnectionMode = ConnectionMode.Gateway,
            HttpClientFactory = () => {
                var innerHandler = new HttpClientHandler {
                    ServerCertificateCustomValidationCallback = 
                        HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                };
                var handler = new CosmosDbHttpClientHandler(port, innerHandler);
                return new HttpClient(handler);
            }
        };

        var cosmosClient = new CosmosClient(AccountEndpoint, AccountKey, cosmosClientOptions);

        // Register the CosmosClient and use the overload that accepts an existing client
        services.AddCosmosDbEventStore(
            cosmosClient,
            "eventstore-test",
            "events",
            "/streamId"
        );

        // EventStore is already registered by AddCosmosDbEventStore, base class will get it automatically
    }

    protected override DockerContainer CreateContainer() {
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

