# Azure Event Hubs Versioning Strategies

This document provides comprehensive documentation for the three versioning strategies available in the Azure Event Hubs Event Store implementation.

## Overview

The Azure Event Hubs Event Store supports three different versioning strategies, each with different trade-offs between performance, consistency, and complexity:

1. **NonAtomicVersionStrategy** - Fast but potentially inconsistent
2. **TableStorageVersionStrategy** - Atomic with strong consistency using Table Storage
3. **BlobLeaseVersionStrategy** - Atomic with strong consistency using Blob Leases

## Strategy Comparison

| Strategy | Consistency | Performance | Complexity | Dependencies | Use Case |
|----------|-------------|-------------|------------|--------------|----------|
| NonAtomicVersionStrategy | Weak (Race conditions possible) | Fastest | Simple | Event Hubs + Blob Storage | Development, Testing, High-throughput scenarios where occasional inconsistencies are acceptable |
| TableStorageVersionStrategy | Strong (Atomic operations) | Medium | Medium | Event Hubs + Blob Storage + Table Storage | Production scenarios requiring strict consistency |
| BlobLeaseVersionStrategy | Strong (Distributed locking) | Medium | Medium | Event Hubs + Blob Storage | Production scenarios requiring strict consistency without Table Storage |

## 1. NonAtomicVersionStrategy

### Overview
The NonAtomicVersionStrategy provides the fastest performance but may skip version validation in certain scenarios. It's designed for high-throughput scenarios where occasional race conditions are acceptable.

### How It Works
1. **Version Detection**: Uses `GetCurrentStreamVersion()` to determine the current stream version
2. **Fallback Behavior**: If version cannot be determined, validation is skipped and the append proceeds
3. **No Atomic Operations**: No distributed locking or atomic operations

### Configuration
```csharp
services.AddAzureEventHubsEventStore(options => {
    options.EnableAtomicVersioning = false; // Uses NonAtomicVersionStrategy
    // ... other options
});
```

### Behavior Characteristics
- **Fast**: No additional Azure service calls for version tracking
- **Potentially Inconsistent**: May skip version validation due to timing issues
- **Race Conditions**: Multiple concurrent appends may succeed when they shouldn't
- **Debugging Friendly**: Shorter timeouts prevent infinite blocking

### When to Use
- Development and testing environments
- High-throughput scenarios where performance is critical
- Scenarios where occasional inconsistencies are acceptable
- Simple deployments without additional Azure services

### Test Results
- `ShouldFailOnWrongVersion` tests may fail intermittently (expected behavior)
- Basic append operations work reliably
- Performance tests show fastest execution times

## 2. TableStorageVersionStrategy

### Overview
The TableStorageVersionStrategy provides strong consistency guarantees using Azure Table Storage with ETag-based conditional updates for atomic optimistic concurrency control.

### How It Works
1. **Version Storage**: Maintains stream versions in Azure Table Storage
2. **Atomic Operations**: Uses ETag-based conditional updates to ensure atomicity
3. **Conflict Detection**: Automatically detects and prevents concurrent modifications
4. **Strong Consistency**: Guarantees that version validation always works correctly

### Configuration
```csharp
services.AddAzureEventHubsEventStore(options => {
    options.EnableAtomicVersioning = true; // Uses atomic versioning
    options.TableStorageConnectionString = "your-table-storage-connection-string";
    // ... other options
});
```

### Behavior Characteristics
- **Strong Consistency**: All version validations work correctly
- **Atomic Operations**: Uses ETags for atomic check-and-update operations
- **No Race Conditions**: Prevents concurrent modifications reliably
- **Medium Performance**: Additional Table Storage calls add latency

### When to Use
- Production environments requiring strict consistency
- Scenarios where race conditions must be prevented
- Applications with Azure Table Storage available
- Critical business logic where data integrity is paramount

### Test Results
- All `ShouldFailOnWrongVersion` tests pass reliably
- Concurrent append operations handled correctly
- Strong consistency guarantees verified

## 3. BlobLeaseVersionStrategy

### Overview
The BlobLeaseVersionStrategy provides strong consistency guarantees using Azure Blob Lease for distributed locking during version checks, without requiring Table Storage.

### How It Works
1. **Version Storage**: Maintains stream versions in blob metadata
2. **Distributed Locking**: Uses blob leases to prevent concurrent modifications
3. **Atomic Operations**: Ensures atomic check-and-update operations via leases
4. **Strong Consistency**: Guarantees that version validation always works correctly

### Configuration
```csharp
services.AddAzureEventHubsEventStore(options => {
    options.EnableAtomicVersioning = true; // Uses atomic versioning
    options.TableStorageConnectionString = null; // Forces BlobLeaseVersionStrategy
    // ... other options
});
```

### Behavior Characteristics
- **Strong Consistency**: All version validations work correctly
- **Distributed Locking**: Uses blob leases for atomic operations
- **No Race Conditions**: Prevents concurrent modifications reliably
- **Medium Performance**: Additional blob lease operations add latency
- **No Table Storage**: Only requires Blob Storage (already needed for Event Hubs Capture)

### When to Use
- Production environments requiring strict consistency
- Scenarios where Azure Table Storage is not available
- Applications with Azure Blob Storage available
- Critical business logic where data integrity is paramount

### Test Results
- All `ShouldFailOnWrongVersion` tests pass reliably
- Concurrent append operations handled correctly
- Strong consistency guarantees verified
- Blob lease acquisition and release tested

## Testing Strategy

### Test Fixtures
Each versioning strategy has its own test fixture:

- `NonAtomicVersionStrategyFixture` - Tests NonAtomicVersionStrategy
- `TableStorageVersionStrategyFixture` - Tests TableStorageVersionStrategy
- `BlobLeaseVersionStrategyFixture` - Tests BlobLeaseVersionStrategy

### Test Classes
Each versioning strategy has its own test class:

- `NonAtomicVersionStrategyTests` - Tests with expected intermittent failures
- `TableStorageVersionStrategyTests` - Tests with strong consistency expectations
- `BlobLeaseVersionStrategyTests` - Tests with blob lease behavior

### Test Coverage
All test classes inherit from `StoreAppendTests<T>` and cover:

- Basic append operations (NoStream, sequential appends)
- Optimistic concurrency control
- Error handling and edge cases
- Strategy-specific behavior

## Implementation Details

### NonAtomicVersionStrategy Implementation
```csharp
public class NonAtomicVersionStrategy : IStreamVersionStrategy {
    public Task<long?> GetVersion(StreamName stream, CancellationToken cancellationToken)
        => _eventStore.GetCurrentStreamVersion(stream, cancellationToken);

    public Task<long> IncrementVersion(StreamName stream, long expectedVersion, int eventCount, CancellationToken cancellationToken) {
        // No-op for non-atomic: version is already incremented by the event append
        return Task.FromResult(expectedVersion + eventCount);
    }
}
```

### TableStorageVersionStrategy Implementation
```csharp
public class TableStorageVersionStrategy : IStreamVersionStrategy {
    public async Task<long> IncrementVersion(StreamName stream, long expectedVersion, int eventCount, CancellationToken cancellationToken) {
        // Get existing entity with ETag
        var response = await _tableClient.GetEntityAsync<TableEntity>(...);

        // Validate expected version
        if (currentVersion != expectedVersion) {
            throw new AppendToStreamException(...);
        }

        // Update with ETag for conditional update
        await _tableClient.UpdateEntityAsync(entity, entity.ETag, ...);
    }
}
```

### BlobLeaseVersionStrategy Implementation
```csharp
public class BlobLeaseVersionStrategy : IStreamVersionStrategy {
    public async Task<long> IncrementVersion(StreamName stream, long expectedVersion, int eventCount, CancellationToken cancellationToken) {
        // Acquire blob lease
        var leaseResponse = await blobClient.AcquireLeaseAsync(...);

        try {
            // Validate and update version atomically
            // ... implementation details
        } finally {
            // Release blob lease
            await blobClient.ReleaseLeaseAsync(leaseResponse.Value.LeaseId);
        }
    }
}
```

## Migration Guide

### From NonAtomicVersionStrategy to Atomic Versioning

1. **Enable Atomic Versioning**:
   ```csharp
   options.EnableAtomicVersioning = true;
   ```

2. **Choose Strategy**:
   - For Table Storage: Provide `TableStorageConnectionString`
   - For Blob Leases: Set `TableStorageConnectionString = null`

3. **Update Tests**: Use appropriate test fixture for the chosen strategy

4. **Verify Behavior**: Run tests to ensure strong consistency

### Performance Considerations

- **NonAtomicVersionStrategy**: Fastest, suitable for high-throughput scenarios
- **TableStorageVersionStrategy**: Medium performance, strong consistency
- **BlobLeaseVersionStrategy**: Medium performance, strong consistency, no Table Storage dependency

## Troubleshooting

### Common Issues

1. **NonAtomicVersionStrategy Tests Failing Intermittently**
   - **Cause**: Race conditions are expected behavior
   - **Solution**: Use atomic versioning strategies for production

2. **TableStorageVersionStrategy ETag Conflicts**
   - **Cause**: Concurrent modifications
   - **Solution**: Retry logic or proper error handling

3. **BlobLeaseVersionStrategy Lease Timeouts**
   - **Cause**: Long-running operations holding leases
   - **Solution**: Optimize lease duration or operation speed

### Debugging Tips

1. **Enable Debug Logging**: Set log level to Debug to see version detection details
2. **Monitor Azure Metrics**: Use Azure Monitor to track performance
3. **Test Isolation**: Use separate test fixtures for each strategy
4. **Timeout Configuration**: Adjust timeouts based on environment

## Conclusion

The Azure Event Hubs Event Store provides three versioning strategies to meet different requirements:

- **NonAtomicVersionStrategy**: Best for development, testing, and high-throughput scenarios
- **TableStorageVersionStrategy**: Best for production with Table Storage available
- **BlobLeaseVersionStrategy**: Best for production without Table Storage dependency

Choose the strategy that best fits your consistency, performance, and infrastructure requirements.
