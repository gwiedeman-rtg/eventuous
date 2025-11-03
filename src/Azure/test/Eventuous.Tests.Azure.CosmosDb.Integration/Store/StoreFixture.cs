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

    protected override void SetupServices(IServiceCollection services) {
        // Get connection details from container
        // Cosmos DB Emulator uses HTTP on port 8081
        var host = Container.Hostname;
        var port = Container.GetMappedPublicPort(8081);

        AccountEndpoint = $"https://{host}:{port}"; // Use HTTPS even though internally it's HTTP
        AccountKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="; // Cosmos DB Emulator default key
        ConnectionString = $"AccountEndpoint={AccountEndpoint};AccountKey={AccountKey};";

        // Add Cosmos DB Event Store
        services.AddCosmosDbEventStore(options => {
            options.AccountEndpoint = AccountEndpoint;
            options.AccountKey = AccountKey;
            options.Database = "eventstore-test";
            options.Container = "events";
            options.PartitionKeyPath = "/streamId";
        });

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

