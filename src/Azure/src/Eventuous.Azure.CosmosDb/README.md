# Eventuous Azure Cosmos DB

Azure Cosmos DB implementation of the Eventuous event store.

## Features

- Full `IEventStore` implementation
- Optimistic concurrency control
- Stream versioning
- Forward and backward event reading
- Stream truncation and deletion
- Transactional batch operations for atomic appends

## Usage

### Basic Setup

```csharp
services.AddCosmosDbEventStore(options => {
    options.ConnectionString = "AccountEndpoint=...;AccountKey=...";
    options.Database = "eventstore";
    options.Container = "events";
    options.PartitionKeyPath = "/streamId";
});
```

### Using Configuration

```json
{
  "Eventuous": {
    "CosmosDb": {
      "ConnectionString": "AccountEndpoint=...;AccountKey=...",
      "Database": "eventstore",
      "Container": "events",
      "PartitionKeyPath": "/streamId"
    }
  }
}
```

```csharp
services.AddCosmosDbEventStore(configuration);
```

### Using Explicit Client

```csharp
var cosmosClient = new CosmosClient(connectionString);
services.AddCosmosDbEventStore(cosmosClient, "eventstore", "events");
```

## Options

- `Database` - Cosmos DB database name (required)
- `Container` - Cosmos DB container name (required)
- `PartitionKeyPath` - Partition key path (default: "/streamId")
- `ConnectionString` - Full connection string (alternative to AccountEndpoint/AccountKey)
- `AccountEndpoint` - Account endpoint URL (alternative to ConnectionString)
- `AccountKey` - Account key (alternative to ConnectionString)

## Partitioning

Events are partitioned by stream ID (`streamId` field), ensuring all events for a single stream are stored together and can be queried efficiently.

## Performance

- Uses transactional batches for atomic event appends
- Optimized queries with partition key specification
- Automatic indexing on all fields


