# Azure Event Hubs Sample

This sample demonstrates how to use Eventuous with Azure Event Hubs as the event store, providing the same functionality as the EventStoreDB and PostgreSQL samples but using Azure cloud services.

## Architecture

This sample uses:
- **Azure Event Hubs** as the primary event store with Event Hubs Capture to Azure Blob Storage
- **Azure Table Storage** for stream metadata and optimistic concurrency control
- **Azure Service Bus** for payment integration events
- **MongoDB** for projections and checkpoints
- **Azurite** for local development (Azure Storage emulator)

## Prerequisites

### For Local Development
- Docker and Docker Compose
- .NET 8.0 SDK
- Azurite (Azure Storage emulator) - included in docker-compose.yml

### For Azure Cloud
- Azure subscription
- Azure Event Hubs namespace with an Event Hub
- Azure Storage Account with Blob Storage and Table Storage
- Azure Service Bus namespace
- Event Hubs Capture enabled to Blob Storage

## Local Development Setup

1. **Start the infrastructure services:**
   ```bash
   docker-compose up -d
   ```

2. **Configure the applications:**
   The sample is pre-configured to use Azurite for local development. No additional configuration is needed.

3. **Run the applications:**
   ```bash
   # Terminal 1 - Bookings API
   cd Bookings
   dotnet run

   # Terminal 2 - Payments service
   cd Bookings.Payments
   dotnet run
   ```

4. **Access the applications:**
   - Bookings API: http://localhost:5000
   - Payments API: http://localhost:5001
   - Swagger UI: http://localhost:5000/swagger

## Azure Cloud Setup

### 1. Create Azure Resources

Create the following Azure resources:

```bash
# Create resource group
az group create --name eventuous-demo --location eastus

# Create Event Hubs namespace
az eventhubs namespace create --name eventuous-events --resource-group eventuous-demo --location eastus

# Create Event Hub
az eventhubs eventhub create --name bookings-events --namespace-name eventuous-events --resource-group eventuous-demo

# Create Storage Account
az storage account create --name eventuousstorage --resource-group eventuous-demo --location eastus --sku Standard_LRS

# Create Service Bus namespace
az servicebus namespace create --name eventuous-bus --resource-group eventuous-demo --location eastus
```

### 2. Enable Event Hubs Capture

```bash
# Enable capture to blob storage
az eventhubs eventhub update --name bookings-events --namespace-name eventuous-events --resource-group eventuous-demo --enable-capture true --capture-destination AzureBlobStorage --capture-container eventhubs-capture --capture-storage-account eventuousstorage
```

### 3. Update Configuration

Update the `appsettings.json` files with your Azure connection strings:

```json
{
  "AzureEventHubs": {
    "ConnectionString": "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY",
    "EventHubName": "bookings-events",
    "ConsumerGroup": "$Default"
  },
  "AzureBlobStorage": {
    "ConnectionString": "DefaultEndpointsProtocol=https;AccountName=yourstorage;AccountKey=YOUR_KEY;EndpointSuffix=core.windows.net",
    "CaptureContainer": "eventhubs-capture"
  },
  "AzureTableStorage": {
    "ConnectionString": "DefaultEndpointsProtocol=https;AccountName=yourstorage;AccountKey=YOUR_KEY;EndpointSuffix=core.windows.net"
  },
  "ServiceBus": {
    "ConnectionString": "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY",
    "TopicName": "payment-events"
  }
}
```

## Key Features

### Optimistic Concurrency Control
The Azure Event Hubs implementation includes proper optimistic concurrency control using Azure Table Storage:
- Stream versions are tracked in Table Storage
- Expected version validation prevents concurrent modifications
- ETag-based atomic updates ensure consistency

### Event Hubs Capture Integration
- Events are automatically captured to Azure Blob Storage
- AVRO format parsing for reading historical events
- Hybrid approach: real-time reading for recent events, capture for historical data

### Native Azure Subscriptions
- **Azure Event Hubs Subscription**: Uses EventProcessorClient for automatic checkpoint management
- **Azure Service Bus Subscription**: For payment integration events
- Automatic load balancing and partition management

## Limitations

- **No Stream Truncation**: Event Hubs doesn't support stream truncation
- **No Stream Deletion**: Event Hubs doesn't support stream deletion
- **No Global Position**: Uses timestamp-based global positions instead of sequence numbers
- **Capture Dependency**: Historical event reading requires Event Hubs Capture to be enabled

## Monitoring

The sample includes comprehensive monitoring:
- **OpenTelemetry** for distributed tracing
- **Prometheus** metrics collection
- **Grafana** dashboards
- **Seq** for structured logging
- **Zipkin** for trace visualization

Access monitoring at:
- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090
- Seq: http://localhost:5341
- Zipkin: http://localhost:9411

## Testing the Sample

1. **Create a booking:**
   ```bash
   curl -X POST "http://localhost:5000/api/bookings" \
     -H "Content-Type: application/json" \
     -d '{
       "RoomId": "room-1",
       "CheckIn": "2024-01-15",
       "CheckOut": "2024-01-20",
       "GuestId": "guest-1"
     }'
   ```

2. **Check projections:**
   - View booking state in MongoDB
   - Check payment integration events in Service Bus

3. **Monitor the system:**
   - Check Grafana dashboards
   - View traces in Zipkin
   - Monitor logs in Seq

## Troubleshooting

### Common Issues

1. **Connection String Issues**
   - Ensure connection strings are properly formatted
   - Check that the Event Hub and Storage Account exist
   - Verify access keys are correct

2. **Capture Not Working**
   - Ensure Event Hubs Capture is enabled
   - Check that the storage account is accessible
   - Verify the container exists

3. **Subscription Issues**
   - Check that consumer groups are properly configured
   - Ensure checkpoints are being stored correctly
   - Verify partition assignment

### Debug Mode

Enable debug logging by setting:
```json
{
  "Logging": {
    "LogLevel": {
      "Eventuous": "Debug",
      "Eventuous.Azure.EventHubs": "Debug"
    }
  }
}
```

## Comparison with Other Samples

This Azure Event Hubs sample provides the same functionality as:
- **EventStoreDB Sample**: Same command/query patterns, different storage
- **PostgreSQL Sample**: Same projections and integrations, different infrastructure

The key difference is the underlying event store implementation, but the application logic remains identical.
