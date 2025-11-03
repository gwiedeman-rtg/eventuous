// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

namespace Eventuous.Azure.CosmosDb;

/// <summary>
/// Options for configuring Cosmos DB Event Store
/// </summary>
public class CosmosDbEventStoreOptions {
    /// <summary>
    /// Cosmos DB database name
    /// </summary>
    public string Database { get; set; } = null!;

    /// <summary>
    /// Cosmos DB container name
    /// </summary>
    public string Container { get; set; } = null!;

    /// <summary>
    /// Partition key path (default: "/streamId")
    /// </summary>
    public string? PartitionKeyPath { get; set; } = "/streamId";

    /// <summary>
    /// Connection string for Cosmos DB
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Account endpoint URL
    /// </summary>
    public string? AccountEndpoint { get; set; }

    /// <summary>
    /// Account key (or use ConnectionString)
    /// </summary>
    public string? AccountKey { get; set; }
}


