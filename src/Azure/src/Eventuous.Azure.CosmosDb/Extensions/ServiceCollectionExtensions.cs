// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Azure.Cosmos;
using System.Net.Http;

namespace Eventuous.Azure.CosmosDb.Extensions;

/// <summary>
/// Service collection extensions for Azure Cosmos DB
/// </summary>
public static class ServiceCollectionExtensions {
    /// <summary>
    /// Add Azure Cosmos DB Event Store to the service collection from configuration
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="configuration">Configuration instance</param>
    /// <param name="sectionName">Configuration section name (default: "Eventuous:CosmosDb")</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddCosmosDbEventStore(
            this IServiceCollection services,
            IConfiguration         configuration,
            string                 sectionName = "Eventuous:CosmosDb"
        ) {
        services.Configure<CosmosDbEventStoreOptions>(
            configuration.GetSection(sectionName)
        );

        services.AddSingleton<CosmosClient>(serviceProvider => {
            var options = serviceProvider.GetRequiredService<IOptions<CosmosDbEventStoreOptions>>().Value;

            CosmosClient client;

            var clientOptions = new CosmosClientOptions {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = false
            };

            // For Cosmos DB Emulator, we may need to disable SSL validation
            if (options.AccountEndpoint?.Contains("localhost") == true ||
                options.AccountEndpoint?.Contains("127.0.0.1") == true ||
                options.ConnectionString?.Contains("localhost") == true ||
                options.ConnectionString?.Contains("127.0.0.1") == true) {
                // Emulator uses self-signed certificate, disable SSL validation for testing
                clientOptions.HttpClientFactory = () => {
                    var handler = new HttpClientHandler();
                    handler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
                    return new HttpClient(handler);
                };
            }

            if (!string.IsNullOrWhiteSpace(options.ConnectionString)) {
                client = new CosmosClient(options.ConnectionString, clientOptions);
            } else if (!string.IsNullOrWhiteSpace(options.AccountEndpoint) && !string.IsNullOrWhiteSpace(options.AccountKey)) {
                client = new CosmosClient(options.AccountEndpoint, options.AccountKey, clientOptions);
            } else {
                throw new InvalidOperationException("Either ConnectionString or both AccountEndpoint and AccountKey must be provided");
            }

            // Initialize database and container if they don't exist
            InitializeCosmosResources(client, options).GetAwaiter().GetResult();

            return client;
        });

        services.AddSingleton<IEventStore>(serviceProvider => {
            var options       = serviceProvider.GetRequiredService<IOptions<CosmosDbEventStoreOptions>>().Value;
            var cosmosClient  = serviceProvider.GetRequiredService<CosmosClient>();
            var serializer    = serviceProvider.GetService<IEventSerializer>();
            var metaSerializer = serviceProvider.GetService<IMetadataSerializer>();
            var logger        = serviceProvider.GetService<ILogger<CosmosDbEventStore>>();

            return new CosmosDbEventStore(cosmosClient, options, serializer, metaSerializer, logger);
        });

        return services;
    }

    /// <summary>
    /// Add Azure Cosmos DB Event Store to the service collection
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="configureOptions">Configuration action for options</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddCosmosDbEventStore(
            this IServiceCollection                  services,
            Action<CosmosDbEventStoreOptions>         configureOptions
        ) {
        services.Configure(configureOptions);

        services.AddSingleton<CosmosClient>(serviceProvider => {
            var options = serviceProvider.GetRequiredService<IOptions<CosmosDbEventStoreOptions>>().Value;

            CosmosClient client;

            var clientOptions = new CosmosClientOptions {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = false
            };

            // For Cosmos DB Emulator, we may need to disable SSL validation
            if (options.AccountEndpoint?.Contains("localhost") == true ||
                options.AccountEndpoint?.Contains("127.0.0.1") == true ||
                options.ConnectionString?.Contains("localhost") == true ||
                options.ConnectionString?.Contains("127.0.0.1") == true) {
                // Emulator uses self-signed certificate, disable SSL validation for testing
                clientOptions.HttpClientFactory = () => {
                    var handler = new HttpClientHandler();
                    handler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
                    return new HttpClient(handler);
                };
            }

            if (!string.IsNullOrWhiteSpace(options.ConnectionString)) {
                client = new CosmosClient(options.ConnectionString, clientOptions);
            } else if (!string.IsNullOrWhiteSpace(options.AccountEndpoint) && !string.IsNullOrWhiteSpace(options.AccountKey)) {
                client = new CosmosClient(options.AccountEndpoint, options.AccountKey, clientOptions);
            } else {
                throw new InvalidOperationException("Either ConnectionString or both AccountEndpoint and AccountKey must be provided");
            }

            // Initialize database and container if they don't exist
            InitializeCosmosResources(client, options).GetAwaiter().GetResult();

            return client;
        });

        services.AddSingleton<IEventStore>(serviceProvider => {
            var options        = serviceProvider.GetRequiredService<IOptions<CosmosDbEventStoreOptions>>().Value;
            var cosmosClient  = serviceProvider.GetRequiredService<CosmosClient>();
            var serializer    = serviceProvider.GetService<IEventSerializer>();
            var metaSerializer = serviceProvider.GetService<IMetadataSerializer>();
            var logger        = serviceProvider.GetService<ILogger<CosmosDbEventStore>>();

            return new CosmosDbEventStore(cosmosClient, options, serializer, metaSerializer, logger);
        });

        return services;
    }

    /// <summary>
    /// Add Azure Cosmos DB Event Store with explicit CosmosClient
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="cosmosClient">Cosmos DB client instance</param>
    /// <param name="database">Database name</param>
    /// <param name="container">Container name</param>
    /// <param name="partitionKeyPath">Partition key path (default: "/streamId")</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddCosmosDbEventStore(
            this IServiceCollection services,
            CosmosClient            cosmosClient,
            string                  database,
            string                  container,
            string?                 partitionKeyPath = "/streamId"
        ) {
        var options = new CosmosDbEventStoreOptions {
            Database         = database,
            Container        = container,
            PartitionKeyPath = partitionKeyPath
        };

        services.AddSingleton(options);

        services.AddSingleton<IEventStore>(serviceProvider => {
            var serializer    = serviceProvider.GetService<IEventSerializer>();
            var metaSerializer = serviceProvider.GetService<IMetadataSerializer>();
            var logger        = serviceProvider.GetService<ILogger<CosmosDbEventStore>>();

            return new CosmosDbEventStore(cosmosClient, options, serializer, metaSerializer, logger);
        });

        return services;
    }

    static async Task InitializeCosmosResources(CosmosClient client, CosmosDbEventStoreOptions options) {
        // Create database if it doesn't exist
        var databaseResponse = await client.CreateDatabaseIfNotExistsAsync(options.Database);

        // Create container if it doesn't exist
        var partitionKeyPath = options.PartitionKeyPath ?? "/streamId";
        var containerProps = new ContainerProperties(options.Container, partitionKeyPath) {
            IndexingPolicy = new IndexingPolicy {
                Automatic     = true,
                IndexingMode  = IndexingMode.Consistent,
                IncludedPaths = {
                    new IncludedPath { Path = "/*" }
                }
            }
        };

        await databaseResponse.Database.CreateContainerIfNotExistsAsync(containerProps);
    }
}

