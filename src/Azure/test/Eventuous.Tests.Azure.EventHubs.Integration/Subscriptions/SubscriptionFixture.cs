// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using Eventuous.Azure.EventHubs.Extensions;
using Eventuous.Azure.EventHubs.Subscriptions;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Filters;
using Eventuous.Tests.Azure.EventHubs.Integration.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Testcontainers.EventHubs;
using TUnit.Core.Interfaces;

namespace Eventuous.Tests.Azure.EventHubs.Integration.Subscriptions;

/// <summary>
/// Test fixture for Azure Event Hubs subscription tests using Docker containers
/// Uses Event Hubs Emulator with internal Azurite for blob/table storage
/// </summary>
public class SubscriptionFixture : IAsyncInitializer, IAsyncDisposable {
    public Testcontainers.EventHubs.EventHubsContainer Container { get; private set; } = null!;
    public ServiceProvider ServiceProvider { get; private set; } = null!;
    public IMessageSubscription Subscription { get; private set; } = null!;
    public string EventHubConnectionString { get; private set; } = null!;
    public string BlobStorageConnectionString { get; private set; } = null!;
    public string TableStorageConnectionString { get; private set; } = null!;

    readonly Action<AzureEventHubsSubscriptionOptions> _configureOptions;
    readonly bool _autoStart;

    public SubscriptionFixture(Action<AzureEventHubsSubscriptionOptions> configureOptions, bool autoStart = true) {
        _configureOptions = configureOptions;
        _autoStart = autoStart;
    }

    public async Task InitializeAsync() {
        var network = EventHubsContainerBuilder.CreateNetwork();

        var azuriteContainer = EventHubsContainerBuilder.Create().WithNetwork(network).Build();

        azuriteContainer.StartAsync().GetAwaiter().GetResult();

        // Initialize Docker container
        Container = EventHubsContainerBuilder.CreateBuilder(network, azuriteContainer)
            .Build();
        await Container.StartAsync();

        // Get connection string from container
        EventHubConnectionString = Container.GetConnectionString();

        // Event Hubs emulator includes internal Azurite, so we use the same connection strings
        // The emulator provides blob and table storage endpoints
        BlobStorageConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
        TableStorageConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";

        var services = new ServiceCollection();

        // Add logging
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Information));

        // Add Azure Event Hubs Event Store
        services.AddAzureEventHubsEventStore(options => {
            options.EventHubConnectionString = EventHubConnectionString;
            options.EventHubName = "test-hub";
            options.BlobStorageConnectionString = BlobStorageConnectionString;
            options.CaptureContainerName = "test-container";
            options.TableStorageConnectionString = TableStorageConnectionString;
            options.ConsumerGroup = "$Default";
            options.UseRealtimeReading = true;
        });

        // Add subscription
        services.AddAzureEventHubsSubscription("test-subscription", _configureOptions);

        ServiceProvider = services.BuildServiceProvider();
        Subscription = ServiceProvider.GetRequiredService<IMessageSubscription>();

        if (_autoStart) {
            await Subscription.Subscribe(
                id => { }, // onSubscribed
                (id, reason, ex) => { }, // onDropped
                CancellationToken.None
            );
        }
    }

    public async ValueTask DisposeAsync() {
        if (Subscription != null) {
            await Subscription.Unsubscribe(id => { }, CancellationToken.None);
            if (Subscription is IAsyncDisposable disposable) {
                await disposable.DisposeAsync();
            }
        }
        await ServiceProvider.DisposeAsync();
        if (Container != null) {
            await Container.DisposeAsync();
        }
    }
}