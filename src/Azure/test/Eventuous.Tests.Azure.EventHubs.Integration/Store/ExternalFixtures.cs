// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Azure.Messaging.EventHubs.Consumer;
using Eventuous;
using Eventuous.Azure.EventHubs.Extensions;
using Eventuous.Azure.EventHubs.Versioning;
using Eventuous.Tests.Persistence.Base.Fixtures;
using Eventuous.TestHelpers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Eventuous.Tests.Azure.EventHubs.Integration.Store;

public abstract class ExternalStoreFixtureBase : StoreFixtureBase, IStartableFixture {
    protected abstract void ConfigureVersionStrategy(ServiceCollection services, string blobConnection, string? tableConnection);
    protected abstract bool ShouldEnableAtomicVersioning(string? tableConnection);

    public string EventHubConnectionString { get; private set; } = null!;
    public string BlobStorageConnectionString { get; private set; } = null!;
    public string TableStorageConnectionString { get; private set; } = null!;

    public async Task InitializeAsync() {
        var services = new ServiceCollection();

        var serializer = new DefaultEventSerializer(TestPrimitives.DefaultOptions, TypeMapper);
        services.AddSingleton<IEventSerializer>(serializer);
        services.AddSingleton(TypeMapper);
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Information));

        var ehConn  = Environment.GetEnvironmentVariable("EVENTHUBS_CONNECTION_STRING") ?? throw new InvalidOperationException("EVENTHUBS_CONNECTION_STRING not set");
        var ehName  = Environment.GetEnvironmentVariable("EVENTHUBS_NAME")               ?? "test-hub";
        var blob    = Environment.GetEnvironmentVariable("BLOB_STORAGE_CONNECTION_STRING") ?? throw new InvalidOperationException("BLOB_STORAGE_CONNECTION_STRING not set");
        var table   = Environment.GetEnvironmentVariable("TABLE_STORAGE_CONNECTION_STRING");
        var group   = Environment.GetEnvironmentVariable("EVENTHUBS_CONSUMER_GROUP")     ?? EventHubConsumerClient.DefaultConsumerGroupName;
        var capture = Environment.GetEnvironmentVariable("CAPTURE_CONTAINER_NAME")        ?? "test-container";

        // Store connection strings for test access
        EventHubConnectionString = ehConn;
        BlobStorageConnectionString = blob;
        TableStorageConnectionString = table ?? string.Empty;

        // Register versioning-specific services
        ConfigureVersionStrategy(services, blob, table);

        // Determine if atomic versioning should be enabled based on the fixture type
        var enableAtomicVersioning = ShouldEnableAtomicVersioning(table);

        // For blob lease strategy, atomic versioning uses blob storage, not table storage
        // So we should only set table connection string if we actually have one
        // This prevents TableServiceClient from being created when using blob lease without table storage
        var tableConnectionString = string.IsNullOrWhiteSpace(table) ? string.Empty : table.Trim();

        // Register EventStore with conditional configuration
        services.AddAzureEventHubsEventStore(options => {
            options.EventHubConnectionString     = ehConn;
            options.EventHubName                 = ehName;
            options.BlobStorageConnectionString  = blob;
            options.CaptureContainerName         = capture;
            options.ConsumerGroup                = group;
            options.UseRealtimeReading           = true;
            options.EnableAtomicVersioning       = enableAtomicVersioning;
            // Explicitly set to empty string - the service registration should check !string.IsNullOrWhiteSpace
            // before creating TableServiceClient. If the check isn't working, the empty string will cause an error,
            // so we need to ensure the check works correctly.
            // For blob lease, atomic versioning is enabled but uses blob storage, so TableStorageConnectionString
            // should be empty and the factory should return null.
            options.TableStorageConnectionString = tableConnectionString;
        });

        Provider   = services.BuildServiceProvider();
        EventStore = Provider.GetRequiredService<IEventStore>();

        if (AutoStart) await Start();
    }

    async Task Start() {
        var hostedServices = Provider.GetServices<IHostedService>();
        foreach (var hostedService in hostedServices) {
            try { await hostedService.StartAsync(CancellationToken.None); } catch { /* ignore */ }
        }
    }

    public async ValueTask DisposeAsync() {
        var inits = Provider.GetServices<IHostedService>();
        foreach (var hostedService in inits) {
            try { await hostedService.StopAsync(CancellationToken.None); } catch { /* ignore */ }
        }
        await Provider.DisposeAsync();
        GC.SuppressFinalize(this);
    }
}

public class ExternalBlobLeaseVersionStrategyFixture : ExternalStoreFixtureBase {
    protected override void ConfigureVersionStrategy(ServiceCollection services, string blobConnection, string? tableConnection) {
        // Blob lease uses blob connection only; no extra registrations needed beyond AddAzureEventHubsEventStore
    }

    protected override bool ShouldEnableAtomicVersioning(string? tableConnection) {
        // Enable atomic versioning for blob lease strategy (uses blob storage, not table storage)
        return true;
    }
}

public class ExternalTableStorageVersionStrategyFixture : ExternalStoreFixtureBase {
    protected override void ConfigureVersionStrategy(ServiceCollection services, string blobConnection, string? tableConnection) {
        if (string.IsNullOrWhiteSpace(tableConnection)) throw new InvalidOperationException("TABLE_STORAGE_CONNECTION_STRING must be set for table strategy");
        // Nothing else required; Event Store will instantiate TableStorageVersionStrategy via options
    }

    protected override bool ShouldEnableAtomicVersioning(string? tableConnection) {
        // Enable atomic versioning for table storage strategy (requires table connection string)
        return !string.IsNullOrWhiteSpace(tableConnection);
    }
}


