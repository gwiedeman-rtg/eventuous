# Azure Event Hubs Integration Tests

This project contains comprehensive integration tests for the Azure Event Hubs Event Store implementation.

## Prerequisites

To run these tests, you need:

1. **Azure Event Hubs Namespace** with:
   - An Event Hub (e.g., `eventuous-test-hub`)
   - Capture enabled and configured to write to Blob Storage
   - Connection string with appropriate permissions

2. **Azure Blob Storage Account** with:
   - A container for Event Hubs Capture (e.g., `eventhubs-capture`)
   - Connection string with appropriate permissions

## Configuration

### Option 1: Environment Variables

Set the following environment variables:

```bash
export Azure__EventHubs__ConnectionString="Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key"
export Azure__EventHubs__EventHubName="eventuous-test-hub"
export Azure__BlobStorage__ConnectionString="DefaultEndpointsProtocol=https;AccountName=youraccount;AccountKey=your-key;EndpointSuffix=core.windows.net"
export Azure__BlobStorage__CaptureContainer="eventhubs-capture"
```

### Option 2: Configuration File

Create `appsettings.test.json` in the test project directory:

```json
{
  "Azure": {
    "EventHubs": {
      "ConnectionString": "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key",
      "EventHubName": "eventuous-test-hub"
    },
    "BlobStorage": {
      "ConnectionString": "DefaultEndpointsProtocol=https;AccountName=youraccount;AccountKey=your-key;EndpointSuffix=core.windows.net",
      "CaptureContainer": "eventhubs-capture"
    }
  }
}
```

## Running the Tests

### All Tests
```bash
dotnet test
```

### Specific Test Categories
```bash
# Run only integration tests
dotnet test --filter "FullyQualifiedName~IntegrationTests"

# Run only serialization tests
dotnet test --filter "FullyQualifiedName~SerializationTests"

# Run only performance tests
dotnet test --filter "FullyQualifiedName~PerformanceTests"
```

### Skip Integration Tests (if you don't have Azure resources)
```bash
dotnet test --filter "FullyQualifiedName!~IntegrationTests&FullyQualifiedName!~PerformanceTests"
```

## Test Categories

### 1. Integration Tests (`AzureEventHubsIntegrationTests`)
- Tests actual Azure Event Hubs and Blob Storage integration
- Validates event publishing and retrieval
- Tests producer functionality
- Requires real Azure resources

### 2. Serialization Tests (`SerializationTests`)
- Validates event and metadata serialization/deserialization
- Tests complex object handling
- Can run without Azure resources

### 3. Performance Tests (`PerformanceTests`)
- Tests throughput and latency characteristics
- Validates memory usage
- Tests concurrent operations
- Requires real Azure resources

### 4. Unit Tests (`AzureEventHubsEventStoreTests`)
- Basic functionality tests
- Configuration validation
- Can run without Azure resources

## Expected Behavior

### Event Publishing
- Events are published to Event Hubs immediately
- Events appear in partitions based on partition key (stream name)
- Batch operations are used for efficiency

### Event Reading
- Real-time events are read directly from Event Hubs
- Historical events are read from Blob Storage (Capture)
- Due to Event Hubs' eventual consistency, there might be delays

### Performance Expectations
- Should handle 100+ events per second
- Batch operations should be efficient
- Memory usage should remain reasonable

## Troubleshooting

### Common Issues

1. **Connection Failures**
   - Verify connection strings are correct
   - Check firewall settings
   - Ensure proper permissions

2. **Events Not Found**
   - Event Hubs has eventual consistency
   - Capture has delays (typically 1-5 minutes)
   - Try waiting longer between publish and read operations

3. **Serialization Errors**
   - Ensure all event types are properly serializable
   - Check for circular references in objects
   - Verify JSON serialization settings

### Debug Logging

Enable debug logging in `appsettings.test.json`:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Eventuous.Azure.EventHubs": "Debug"
    }
  }
}
```

## CI/CD Considerations

For automated testing in CI/CD pipelines:

1. Use Azure Service Principal authentication
2. Create dedicated test resources
3. Clean up test data after runs
4. Consider using Azure Resource Manager templates for test infrastructure

## Security Notes

- Never commit connection strings to source control
- Use Azure Key Vault for production scenarios
- Rotate access keys regularly
- Use minimal required permissions