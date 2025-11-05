// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Testcontainers.CosmosDb;

namespace Eventuous.Tests.Azure.CosmosDb.Integration.Fixtures;

/// <summary>
/// Helper class for creating Azure Cosmos DB emulator containers
/// </summary>
public static class CosmosDbContainerBuilder {
    /// <summary>
    /// Creates a new CosmosDB emulator container with default configuration for testing
    /// Uses the official Testcontainers.CosmosDb package which handles emulator quirks
    /// </summary>
    /// <returns>Configured CosmosDB container</returns>
    public static CosmosDbContainer Create() {
        // The CosmosDbBuilder from Testcontainers.CosmosDb handles the emulator setup
        // including SSL certificate handling and port mapping
        return new CosmosDbBuilder()
            .WithImage("mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest")
            .Build();
    }
}

