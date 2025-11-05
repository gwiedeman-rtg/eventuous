// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Net.Http;
using Eventuous.Azure.CosmosDb;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Eventuous.Azure.CosmosDb.Extensions;

public static class ServiceCollectionExtensions {
    /// <summary>
    /// Adds Cosmos DB Event Store to the service collection
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="configure">Configuration action for options</param>
    /// <returns>The service collection for chaining</returns>
    public static IServiceCollection AddCosmosDbEventStore(
        this IServiceCollection services,
        Action<CosmosDbEventStoreOptions> configure
    ) {
        var options = new CosmosDbEventStoreOptions();
        configure(options);

        // Register options
        services.AddSingleton(options);

        // Register CosmosClient as singleton with initialization
        services.AddSingleton<CosmosClient>(serviceProvider => {
            // Configure custom serializer that respects JsonPropertyName attributes
            var jsonOptions = new System.Text.Json.JsonSerializerOptions {
                PropertyNamingPolicy = null // Use exact JsonPropertyName values, don't apply camelCase
            };
            var customSerializer = new CosmosJsonSerializer(jsonOptions);

            var cosmosClientOptions = new CosmosClientOptions {
                ConnectionMode = ConnectionMode.Gateway,
                Serializer = customSerializer,
                // For local emulator, we need to bypass SSL validation
                // This is safe for local development but should never be used in production
                HttpClientFactory = () => {
                    HttpMessageHandler httpMessageHandler = new HttpClientHandler {
                        ServerCertificateCustomValidationCallback =
                            HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                    };
                    return new HttpClient(httpMessageHandler);
                }
            };

            CosmosClient client;

            if (!string.IsNullOrEmpty(options.ConnectionString)) {
                client = new CosmosClient(options.ConnectionString, cosmosClientOptions);
            }
            else if (!string.IsNullOrEmpty(options.AccountEndpoint) && !string.IsNullOrEmpty(options.AccountKey)) {
                client = new CosmosClient(options.AccountEndpoint, options.AccountKey, cosmosClientOptions);
            }
            else {
                throw new InvalidOperationException(
                    "Either ConnectionString or both AccountEndpoint and AccountKey must be provided in CosmosDbEventStoreOptions"
                );
            }

            // Create database and container if they don't exist
            // This is done synchronously during startup - intentional for initialization
            InitializeCosmosResources(client, options).GetAwaiter().GetResult();

            return client;
        });

        // Register Event Store using AddEventStore to register interfaces properly
        services.AddEventStore<CosmosDbEventStore>(serviceProvider => {
            var cosmosClient = serviceProvider.GetRequiredService<CosmosClient>();
            var logger = serviceProvider.GetService<ILogger<CosmosDbEventStore>>();
            var serializer = serviceProvider.GetService<IEventSerializer>();
            var metaSerializer = serviceProvider.GetService<IMetadataSerializer>();

            return new CosmosDbEventStore(
                cosmosClient,
                options,
                serializer,
                metaSerializer,
                logger
            );
        });

        return services;
    }

    /// <summary>
    /// Adds Cosmos DB Event Store to the service collection using configuration
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="configuration">Configuration section containing Cosmos DB options</param>
    /// <param name="sectionName">Name of the configuration section (default: "Eventuous:CosmosDb")</param>
    /// <returns>The service collection for chaining</returns>
    public static IServiceCollection AddCosmosDbEventStore(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = "Eventuous:CosmosDb"
    ) {
        var section = configuration.GetSection(sectionName);
        var options = section.Get<CosmosDbEventStoreOptions>()
            ?? throw new InvalidOperationException($"Configuration section '{sectionName}' not found or invalid");

        return services.AddCosmosDbEventStore(opt => {
            opt.Database = options.Database;
            opt.Container = options.Container;
            opt.PartitionKeyPath = options.PartitionKeyPath;
            opt.ConnectionString = options.ConnectionString;
            opt.AccountEndpoint = options.AccountEndpoint;
            opt.AccountKey = options.AccountKey;
        });
    }

    /// <summary>
    /// Adds Cosmos DB Event Store to the service collection using an existing CosmosClient
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="cosmosClient">Existing CosmosClient instance</param>
    /// <param name="database">Database name</param>
    /// <param name="container">Container name</param>
    /// <param name="partitionKeyPath">Partition key path (default: "/streamId")</param>
    /// <returns>The service collection for chaining</returns>
    public static IServiceCollection AddCosmosDbEventStore(
        this IServiceCollection services,
        CosmosClient cosmosClient,
        string database,
        string container,
        string partitionKeyPath = "/streamId"
    ) {
        var options = new CosmosDbEventStoreOptions {
            Database = database,
            Container = container,
            PartitionKeyPath = partitionKeyPath
        };

        services.AddSingleton(options);

        // Register the provided CosmosClient WITHOUT initializing resources
        // Resources should be initialized after the emulator is ready (e.g., in test fixtures)
        services.AddSingleton(cosmosClient);

        // Register Event Store using AddEventStore to register interfaces properly
        services.AddEventStore<CosmosDbEventStore>(serviceProvider => {
            var logger = serviceProvider.GetService<ILogger<CosmosDbEventStore>>();
            var serializer = serviceProvider.GetService<IEventSerializer>();
            var metaSerializer = serviceProvider.GetService<IMetadataSerializer>();

            return new CosmosDbEventStore(
                cosmosClient,
                options,
                serializer,
                metaSerializer,
                logger
            );
        });

        return services;
    }

    /// <summary>
    /// Initializes Cosmos DB database and container if they don't exist
    /// </summary>
    private static async Task InitializeCosmosResources(CosmosClient client, CosmosDbEventStoreOptions options) {
        try {
            // Create database if not exists
            var databaseResponse = await client.CreateDatabaseIfNotExistsAsync(options.Database);
            var database = databaseResponse.Database;

            // Create container if not exists
            // The partition key path must be specified at creation time
            var partitionKeyPath = options.PartitionKeyPath ?? "/streamId";
            var containerProperties = new ContainerProperties {
                Id = options.Container,
                PartitionKeyPath = partitionKeyPath
            };

            var containerResponse = await database.CreateContainerIfNotExistsAsync(containerProperties);

            // Verify the container was created successfully
            if (containerResponse.StatusCode != System.Net.HttpStatusCode.Created &&
                containerResponse.StatusCode != System.Net.HttpStatusCode.OK) {
                throw new InvalidOperationException(
                    $"Failed to create container '{options.Container}'. Status: {containerResponse.StatusCode}"
                );
            }

            // Verify container exists and has correct partition key by reading its metadata
            var container = database.GetContainer(options.Container);
            var containerReadResponse = await container.ReadContainerAsync();

            // Verify partition key path matches
            if (containerReadResponse.Resource.PartitionKeyPath != partitionKeyPath) {
                throw new InvalidOperationException(
                    $"Container '{options.Container}' exists but has wrong partition key path. " +
                    $"Expected: '{partitionKeyPath}', Actual: '{containerReadResponse.Resource.PartitionKeyPath}'"
                );
            }
        } catch (Exception ex) {
            throw new InvalidOperationException(
                $"Failed to initialize Cosmos DB resources. Database: '{options.Database}', Container: '{options.Container}'. " +
                "Ensure the Cosmos DB emulator is running and accessible.",
                ex
            );
        }
    }
}

