// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.Azurite;
using Testcontainers.EventHubs;

namespace Eventuous.Tests.Azure.EventHubs.Integration.Fixtures;

/// <summary>
/// Helper class for creating Azure Event Hubs emulator containers
/// </summary>
public static class EventHubsContainerBuilder {
    /// <summary>
    /// Creates a new EventHubsBuilder with default configuration for testing
    /// Uses a separate Azurite container for better reliability
    /// </summary>
    /// <returns>Configured EventHubsBuilder</returns>
    public static EventHubsBuilder CreateBuilder()
        => new EventHubsBuilder()
            //.WithImage("mcr.microsoft.com/azure-messaging/eventhubs-emulator:2.0.1")
            .WithAcceptLicenseAgreement(true)
            //.WithAzuriteContainer(network, azurite, "eventhubs_test_network")
            .WithConfigurationBuilder(GetServiceConfiguration())
            //.WithEnvironment("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
            //.WithWaitStrategy(Wait.ForUnixContainer()
            //    .WithStartupCallback((container, ct) =>
            //        Task.Delay(TimeSpan.FromSeconds(120), ct))); // Give more time for emulator to start
            ;

    /// <summary>
    /// Creates a simple EventHubsBuilder for use with real Azure Event Hubs
    /// This bypasses the emulator issues by using the actual service
    /// </summary>
    /// <returns>Configured EventHubsBuilder</returns>
    public static EventHubsBuilder CreateSimpleBuilder()
        => new EventHubsBuilder()
            //.WithImage("mcr.microsoft.com/azure-messaging/eventhubs-emulator:2.0.1")
            .WithAcceptLicenseAgreement(true)
            .WithConfigurationBuilder(GetServiceConfiguration());

    /// <summary>
    /// Creates the service configuration for Event Hubs testing
    /// </summary>
    /// <returns>EventHubsServiceConfiguration</returns>
    private static EventHubsServiceConfiguration GetServiceConfiguration() {
        return EventHubsServiceConfiguration.Create()
            .WithEntity("test-hub", 2, "$Default", "test-consumer-group");
    }

    /// <summary>
    /// Creates a new network for Event Hubs and Azurite containers
    /// </summary>
    /// <returns>Configured network name</returns>
    public static INetwork CreateNetwork()
        => new NetworkBuilder()
            .WithName($"eventhubs_test_network_{Guid.NewGuid()}")
            .Build();

    /// <summary>
    /// Creates a new AzuriteContainer with default configuration for testing
    /// </summary>
    /// <returns>Configured AzuriteContainer</returns>
    public static AzuriteBuilder Create()
        => new AzuriteBuilder()
            .WithImage("mcr.microsoft.com/azure-storage/azurite:3.33.0")
            .WithExposedPort(10000)
            .WithExposedPort(10001)
            .WithExposedPort(10002);
}

