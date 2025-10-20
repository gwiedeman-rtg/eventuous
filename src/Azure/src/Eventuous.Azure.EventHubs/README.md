# Eventuous Azure Event Hubs

This package provides an Azure Event Hubs-based implementation of the Eventuous event store interfaces.

## Features

- Implements `IEventStore`, `IEventReader`, and `IEventWriter` interfaces
- Uses Azure Event Hubs with Capture for event storage and retrieval
- Supports stream-based event organization using partition keys
- Leverages Azure Blob Storage for captured event data reading
- Provides both real-time event production and historical event reading capabilities

## Usage

### Basic Setup

```csharp
using Eventuous.Azure.EventHubs;

// Create the event store
var eventStore = new AzureEventHubsEventStore(
    eventHubConnectionString: "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key",
    eventHubName: "your-event-hub-name",
    blobStorageConnectionString: "DefaultEndpointsProtocol=https;AccountName=youraccount;AccountKey=your-key;EndpointSuffix=core.windows.net",
    captureContainerName: "eventhubs-capture"
);

// Use as any other IEventStore implementation
var streamName = new StreamName("user-123");
var events = new[] {
    new NewStreamEvent(Guid.NewGuid(), new UserCreated("123", "John Doe"), new Metadata())
};

await eventStore.AppendEvents(streamName, ExpectedStreamVersion.NoStream, events);
var readEvents = await eventStore.ReadEvents(streamName, StreamReadPosition.Start, 10, false);
```

### Using with Dependency Injection

```csharp
using Eventuous.Azure.EventHubs.Extensions;

// In your Program.cs or Startup.cs
services.AddAzureEventHubsEventStore(options => {
    options.EventHubConnectionString = "your-connection-string";
    options.EventHubName = "your-event-hub";
    options.BlobStorageConnectionString = "your-blob-connection-string";
    options.CaptureContainerName = "eventhubs-capture";
});

// Or register the producer separately
services.AddAzureEventHubsProducer("your-connection-string", "your-event-hub");
```

### Using the Producer

```csharp
using Eventuous.Azure.EventHubs;
using Eventuous.Producers;

var producer = new AzureEventHubsProducer(
    connectionString: "your-event-hubs-connection-string",
    eventHubName: "your-event-hub-name"
);

var messages = new[] {
    new ProducedMessage(new UserCreated("123", "John Doe"), new Metadata())
};

await producer.Produce(new StreamName("user-events"), messages);
```

## Configuration

The Azure Event Hubs Event Store requires:
- An Azure Event Hubs namespace and event hub
- Azure Event Hubs Capture configured to store events in Azure Blob Storage
- Appropriate connection strings and permissions