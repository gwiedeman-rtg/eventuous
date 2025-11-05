# Bookings sample application - Azure Cosmos DB

This project demonstrates some of the features of Eventuous:
 * Event-sourced domain model using `Aggregate` and `AggregateState`
 * Aggregate persistence using Azure Cosmos DB
 * Read models in MongoDB (for projections - when subscriptions are available)

## Usage

Start the infrastructure using Docker Compose from this directory:

```bash
docker compose up
```

**Note**: The Cosmos DB emulator may take a few minutes to start. Wait until the health check passes before running the application.

Run the `Bookings` project and then open `http://localhost:5051/swagger/index.html`. 
Here you can use SwaggerUI to initiate commands that will result in events being raised.

### Example commands

#### Bookings -> BookRoom (`/booking/book`)

- This command raises an event, which gets stored in Cosmos DB.
- The event is persisted in the Cosmos DB container using the configured partition key.

### Configuration

The application uses the Cosmos DB emulator connection string by default. Update `appsettings.json` if you want to use a different Cosmos DB instance:

```json
{
  "Eventuous": {
    "CosmosDb": {
      "ConnectionString": "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
      "Database": "eventstore",
      "Container": "events",
      "PartitionKeyPath": "/streamId"
    }
  }
}
```

### Current Limitations

- **Subscriptions**: Cosmos DB subscriptions are not yet implemented. When available, you'll be able to subscribe to events for projections and read models.
- **Read Models**: Currently, read models/projections are not available since subscriptions are required. This will be added when Cosmos DB subscriptions are implemented.

### Architecture

```mermaid
graph TB
    HTTP --> BookRoom
    subgraph Bookings 
    direction LR
    BookRoom -- aggregate --> RoomBooked[RoomBooked<br>domain event]
    end
    subgraph CosmosDB
    RoomBooked -- eventstore --> CosmosDB[(Cosmos DB)]
    end
```

## Cosmos DB Emulator

The sample uses the Azure Cosmos DB Emulator running in Docker. The emulator provides:
- Endpoint: `https://localhost:8081/`
- Default key: `C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==`

For production use, replace the connection string with your Azure Cosmos DB account details.

