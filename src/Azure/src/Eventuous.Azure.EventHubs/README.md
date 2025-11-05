# Eventuous Azure Event Hubs

This package provides an Azure Event Hubs-based implementation of the Eventuous event store interfaces with full optimistic concurrency control and native Azure subscriptions.

## Features

- Implements `IEventStore`, `IEventReader`, and `IEventWriter` interfaces
- **Optimistic Concurrency Control**: Uses Azure Table Storage for stream versioning and conflict detection
- **Proper AVRO Parsing**: Correctly deserializes Event Hubs Capture files using Apache AVRO
- **Native Azure Subscriptions**: EventProcessorClient-based subscriptions with automatic checkpointing
- **Stream Isolation**: Uses partition keys to ensure stream events are co-located
- **Hybrid Reading**: Real-time reading for recent events, capture files for historical data
- **Azure Service Bus Integration**: Native support for Service Bus subscriptions and producers

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
- Azure Table Storage for stream metadata and concurrency control
- Appropriate connection strings and permissions

### Required Configuration

```csharp
services.AddAzureEventHubsEventStore(options => {
    options.EventHubConnectionString = "your-event-hubs-connection-string";
    options.EventHubName = "your-event-hub-name";
    options.BlobStorageConnectionString = "your-blob-storage-connection-string";
    options.TableStorageConnectionString = "your-table-storage-connection-string";
    options.CaptureContainerName = "eventhubs-capture";
});
```

## Key Fixes and Improvements

### 1. Optimistic Concurrency Control ✅
- **Fixed**: Added proper version validation using Azure Table Storage
- **Implementation**: ETag-based atomic updates prevent concurrent modifications
- **Alignment**: Matches EventStoreDB and PostgreSQL concurrency behavior

### 2. AVRO Parsing ✅
- **Fixed**: Replaced text-based parsing with proper Apache AVRO deserialization
- **Implementation**: Uses `DataFileReader<GenericRecord>` for binary AVRO files
- **Benefits**: Robust parsing of Event Hubs Capture files

### 3. Stream Version Tracking ✅
- **Fixed**: Added stream position tracking in event properties
- **Implementation**: Stores `StreamPosition` in event metadata
- **Benefits**: Consistent stream versioning across all operations

### 4. Partition Key Consistency ✅
- **Fixed**: Ensured consistent partition key usage for stream isolation
- **Implementation**: Uses stream name as partition key for all batch operations
- **Benefits**: Events for the same stream are always co-located

### 5. Native Azure Subscriptions ✅
- **Added**: `AzureEventHubsSubscription` using EventProcessorClient
- **Features**: Automatic checkpoint management, load balancing, error handling
- **Integration**: Seamless integration with Eventuous subscription framework

## Limitations

### Event Hubs Limitations
- **No Stream Truncation**: Event Hubs doesn't support stream truncation
- **No Stream Deletion**: Event Hubs doesn't support stream deletion
- **No Global Position**: Uses timestamp-based positions instead of sequence numbers

### Azure-Specific Considerations
- **Capture Dependency**: Historical reading requires Event Hubs Capture to be enabled
- **Storage Costs**: Table Storage and Blob Storage have associated costs
- **Latency**: Network calls to Azure services add latency compared to local stores

### Workarounds
- **Stream Truncation**: Not supported - use event versioning instead
- **Stream Deletion**: Not supported - use data retention policies
- **Global Position**: Timestamp-based positions work for most use cases

## Migration from Other Event Stores

### From EventStoreDB
```csharp
// Before (EventStoreDB)
services.AddEventStoreClient(connectionString);
services.AddEventStore<EsdbEventStore>();

// After (Azure Event Hubs)
services.AddAzureEventHubsEventStore(options => {
    options.EventHubConnectionString = connectionString;
    options.EventHubName = "your-event-hub";
    options.BlobStorageConnectionString = blobConnectionString;
    options.TableStorageConnectionString = tableConnectionString;
    options.CaptureContainerName = "eventhubs-capture";
});
```

### From PostgreSQL
```csharp
// Before (PostgreSQL)
services.AddNpgsqlDataSource(connectionString);
services.AddEventStore<PostgresStore>();

// After (Azure Event Hubs)
services.AddAzureEventHubsEventStore(options => {
    // Same configuration as above
});
```

## Performance Considerations

- **Batch Size**: Configure appropriate batch sizes for your throughput requirements
- **Partition Count**: More partitions = higher throughput but more complexity
- **Capture Frequency**: Balance between latency and storage costs
- **Checkpoint Frequency**: Balance between performance and recovery time

## Monitoring and Diagnostics

The implementation includes comprehensive logging and metrics:
- Stream version conflicts
- AVRO parsing errors
- Partition assignment changes
- Checkpoint operations
- Event processing latency

Use Azure Monitor, Application Insights, or your preferred monitoring solution to track these metrics.