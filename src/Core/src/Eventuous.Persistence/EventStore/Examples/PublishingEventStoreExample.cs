// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Handlers;

namespace Eventuous.Examples;

/// <summary>
/// Example demonstrating how to use PublishingEventStore with event handlers
/// </summary>
public static class PublishingEventStoreExample {
    /// <summary>
    /// Basic usage example
    /// </summary>
    public static async Task BasicUsageExample() {
        // Create handler registry and event store
        var registry = new EventStoreHandlerRegistry();
        var eventStore = new PublishingEventStore(registry);

        // Create and register handlers
        var auditHandler = new DelegateEventHandler("AuditHandler", (streamName, streamEvent) => {
            Console.WriteLine($"AUDIT: Event {streamEvent.Id} appended to stream {streamName}");
        });

        var notificationHandler = new DelegateEventHandler("NotificationHandler", async (streamName, streamEvent, ct) => {
            // Simulate async work like sending notifications
            await Task.Delay(10, ct);
            Console.WriteLine($"NOTIFICATION: New event in stream {streamName}");
        });

        // Register handlers using extension methods
        eventStore
            .RegisterHandler(auditHandler)
            .RegisterHandler(notificationHandler);

        // Append events - handlers will be called automatically
        var streamName = new StreamName("user-123");
        var events = new[] {
            new NewStreamEvent(Guid.NewGuid(), new UserRegistered("john@example.com"), new Metadata().With("CorrelationId", Guid.NewGuid())),
            new NewStreamEvent(Guid.NewGuid(), new UserEmailVerified("john@example.com"), new Metadata().With("CorrelationId", Guid.NewGuid()))
        };

        await eventStore.AppendEvents(streamName, ExpectedStreamVersion.NoStream, events, CancellationToken.None);

        Console.WriteLine("Events appended and handlers notified!");
    }

    /// <summary>
    /// Advanced configuration example
    /// </summary>
    public static async Task AdvancedConfigurationExample() {
        // Create registry and event store with custom options
        var registry = new EventStoreHandlerRegistry();
        var options = new PublishingEventStoreOptions {
            ContinueOnHandlerFailure = true,
            PublishConcurrently = true  // Process handlers concurrently
        };
        var eventStore = new PublishingEventStore(registry, options);

        // Add multiple handlers
        var emailHandler = new EmailNotificationHandler();
        var auditHandler = new DelegateEventHandler("AuditHandler", (streamName, streamEvent) => {
            Console.WriteLine($"AUDIT: Event {streamEvent.Id} of type {streamEvent.Payload?.GetType().Name} appended to stream {streamName}");
        });
        var loggingHandler = new LoggingEventHandler("EventLogger");

        eventStore
            .RegisterHandler(emailHandler)
            .RegisterHandler(auditHandler)
            .RegisterHandler(loggingHandler);

        // Process events
        var streamName = new StreamName("order-456");
        var events = new[] {
            new NewStreamEvent(Guid.NewGuid(), new OrderPlaced("ORDER-123", 99.99m), new Metadata())
        };

        await eventStore.AppendEvents(streamName, ExpectedStreamVersion.NoStream, events, CancellationToken.None);

        Console.WriteLine("Events processed with advanced configuration!");
    }

    // Example event types
    public record UserRegistered(string Email);
    public record UserEmailVerified(string Email);
    public record OrderPlaced(string OrderId, decimal Amount);

    // Example handler implementation
    public class EmailNotificationHandler : IEventStoreHandler {
        public string Name => "EmailNotificationHandler";

        public Task HandleEvent(StreamName streamName, StreamEvent streamEvent, CancellationToken cancellationToken = default) {
            // Handle different event types
            switch (streamEvent.Payload) {
                case UserRegistered userRegistered:
                    Console.WriteLine($"Sending welcome email to {userRegistered.Email}");
                    // Send welcome email logic here
                    break;

                case OrderPlaced orderPlaced:
                    Console.WriteLine($"Sending order confirmation for {orderPlaced.OrderId}");
                    // Send order confirmation logic here
                    break;

                default:
                    Console.WriteLine($"No email notification needed for event type {streamEvent.Payload?.GetType().Name}");
                    break;
            }

            return Task.CompletedTask;
        }
    }
}