// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using Eventuous.Azure.EventHubs.Extensions;
using Eventuous.Azure.EventHubs.Subscriptions;
using Eventuous.Subscriptions;
using Eventuous.Subscriptions.Filters;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using TUnit.Core.Interfaces;

namespace Eventuous.Tests.Azure.EventHubs.Integration.Subscriptions;

/// <summary>
/// Test fixture for Azure Event Hubs subscription tests
/// TODO: Add Docker container support for Event Hubs Emulator + Azurite
/// For now, uses mock connection strings for basic test structure
/// </summary>
public class SubscriptionFixture : IAsyncInitializer, IAsyncDisposable {
    public ServiceProvider ServiceProvider { get; private set; } = null!;
    public IMessageSubscription Subscription { get; private set; } = null!;

    readonly Action<AzureEventHubsSubscriptionOptions> _configureOptions;
    readonly bool _autoStart;

    public SubscriptionFixture(Action<AzureEventHubsSubscriptionOptions> configureOptions, bool autoStart = true) {
        _configureOptions = configureOptions;
        _autoStart = autoStart;
    }

    public async Task InitializeAsync() {
        // TODO: Initialize Docker containers (Event Hubs Emulator + Azurite)
        // For now, use mock connection strings
        var eventHubConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
        var blobStorageConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
        var tableStorageConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";

        var services = new ServiceCollection();

        // Add logging
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Information));

        // Add Azure Event Hubs Event Store
        services.AddAzureEventHubsEventStore(options => {
            options.EventHubConnectionString = eventHubConnectionString;
            options.EventHubName = "test-hub";
            options.BlobStorageConnectionString = blobStorageConnectionString;
            options.CaptureContainerName = "test-container";
            options.TableStorageConnectionString = tableStorageConnectionString;
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
    }
}
