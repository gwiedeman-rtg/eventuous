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
    /// </summary>
    /// <returns>Configured EventHubsBuilder</returns>
    public static EventHubsBuilder CreateBuilder()
        => new EventHubsBuilder()
            .WithImage("mcr.microsoft.com/azure-messaging/eventhubs-emulator:latest")
            .WithAcceptLicenseAgreement(true)
            .WithAzuriteContainer(EventHubsNetworkBuilder.CreateNetwork(), AzuriteContainerBuilder.Create().Build(), "eventhubs_test_network")

        ;
}

public static class AzuriteContainerBuilder {
    /// <summary>
    /// Creates a new AzuriteContainer with default configuration for testing
    /// </summary>
    /// <returns>Configured AzuriteContainer</returns>
    public static AzuriteBuilder Create()
        => new AzuriteBuilder()
            .WithImage("mcr.microsoft.com/azure-storage/azurite:latest")
            .WithExposedPort(10000)
            .WithExposedPort(10001)
            .WithExposedPort(10002);
}

public static class EventHubsNetworkBuilder {
    /// <summary>
    /// Creates a new network for Event Hubs and Azurite containers
    /// </summary>
    /// <returns>Configured network name</returns>
    public static INetwork CreateNetwork()
        => new NetworkBuilder()
            .WithName($"eventhubs_test_network_{Guid.NewGuid()}")
            .Build();
}
