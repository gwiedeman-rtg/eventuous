// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Testcontainers.CosmosDb;

namespace Eventuous.Tests.Azure.CosmosDb.Integration.Fixtures;

/// <summary>
/// Helper class for creating Azure Cosmos DB emulator containers
/// </summary>
public static class CosmosDbContainerBuilder {
    /// <summary>
    /// Creates a new CosmosDB emulator container with default configuration for testing
    /// </summary>
    /// <returns>Configured container</returns>
    public static DockerContainer Create() {
        // Azure Cosmos DB Emulator runs on Linux and requires specific ports
        // Port 8081 is the SQL API endpoint (HTTP)
        // The emulator uses HTTPS internally but we can access via HTTP
        return new CosmosDbBuilder()
            .WithImage("mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest")
            .WithEnvironment("AZURE_COSMOS_EMULATOR_PARTITION_COUNT", "10")
            .WithEnvironment("AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE", "false")
            .WithEnvironment("AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE", "127.0.0.1")
            .WithPortBinding(8081, true) // Use random port to avoid conflicts
            .WithWaitStrategy(
                Wait.ForUnixContainer()
                    // Wait for the emulator to log that it's ready
                    // The emulator logs various startup messages - we wait for any indication of readiness
                    // This pattern matches common startup completion messages
                    .UntilMessageIsLogged("(?s).*(Started|started|ready|Ready|listening).*")
            )
            .Build();
    }
}

