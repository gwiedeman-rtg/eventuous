# Azure Event Hubs Versioning Strategy Tests

This directory contains comprehensive integration tests for all three versioning strategies available in the Azure Event Hubs Event Store implementation.

## Test Structure

### Test Fixtures
Each versioning strategy has its own test fixture:

- **`NonAtomicVersionStrategyFixture`** - Tests the NonAtomicVersionStrategy
- **`TableStorageVersionStrategyFixture`** - Tests the TableStorageVersionStrategy
- **`BlobLeaseVersionStrategyFixture`** - Tests the BlobLeaseVersionStrategy

### Test Classes
Each versioning strategy has its own test class:

- **`NonAtomicVersionStrategyTests`** - Tests with expected intermittent failures
- **`TableStorageVersionStrategyTests`** - Tests with strong consistency expectations
- **`BlobLeaseVersionStrategyTests`** - Tests with blob lease behavior

## Running the Tests

### Prerequisites
- Docker Desktop running
- .NET SDK installed
- Testcontainers support enabled

### Running All Tests
```bash
dotnet test src/Azure/test/Eventuous.Tests.Azure.EventHubs.Integration/
```

### Running Specific Strategy Tests
```bash
# NonAtomicVersionStrategy tests
dotnet test --filter "Category=NonAtomicVersionStrategy"

# TableStorageVersionStrategy tests
dotnet test --filter "Category=TableStorageVersionStrategy"

# BlobLeaseVersionStrategy tests
dotnet test --filter "Category=BlobLeaseVersionStrategy"
```

### Running Individual Test Classes
```bash
# NonAtomicVersionStrategy tests
dotnet test --filter "ClassName=NonAtomicVersionStrategyTests"

# TableStorageVersionStrategy tests
dotnet test --filter "ClassName=TableStorageVersionStrategyTests"

# BlobLeaseVersionStrategy tests
dotnet test --filter "ClassName=BlobLeaseVersionStrategyTests"
```

## Expected Test Results

### NonAtomicVersionStrategyTests
- ✅ Basic append operations (NoStream, sequential appends)
- ⚠️ `ShouldFailOnWrongVersion` tests may fail intermittently (expected behavior)
- ✅ Performance tests show fastest execution times
- ⚠️ Race conditions are possible due to non-atomic operations

### TableStorageVersionStrategyTests
- ✅ All optimistic concurrency tests pass reliably
- ✅ Strong consistency guarantees verified
- ✅ Concurrent append operations handled correctly
- ✅ Atomic operations using ETag-based conditional updates

### BlobLeaseVersionStrategyTests
- ✅ All optimistic concurrency tests pass reliably
- ✅ Strong consistency guarantees verified
- ✅ Concurrent append operations handled correctly
- ✅ Atomic operations using blob lease distributed locking

## Test Infrastructure

### Docker Containers
Each test fixture uses Docker containers for real integration testing:

- **Event Hubs Emulator** - For Event Hubs operations
- **Azurite** - For Azure Storage (Blob Storage and Table Storage)

### Network Configuration
- Containers run in isolated Docker networks
- Port mappings for external access during debugging
- Automatic cleanup after test completion

## Debugging

### Enabling Debug Logging
Set the log level to Debug to see detailed version detection information:

```csharp
public NonAtomicVersionStrategyFixture() : base(LogLevel.Debug) { }
```

### Debugging Event Hubs Consumer Issues
The Event Hubs Consumer now has proper timeout configuration:
- `MaximumWaitTime = TimeSpan.FromSeconds(1)` - Short timeout for debugging
- `combinedCts.CancelAfter(readTimeout)` - Prevents infinite blocking

### Debugging Version Strategy Issues
Each strategy logs detailed information about:
- Version detection attempts
- Fallback scenarios
- Atomic operation results
- Error conditions

## Troubleshooting

### Common Issues

1. **Docker Container Startup Failures**
   - Ensure Docker Desktop is running
   - Check available disk space
   - Verify port availability (9093, 10000, 10001, 10002)

2. **NonAtomicVersionStrategy Test Failures**
   - Intermittent failures are expected behavior
   - Use atomic versioning strategies for production

3. **TableStorageVersionStrategy ETag Conflicts**
   - May occur with concurrent test execution
   - Tests are designed to handle this scenario

4. **BlobLeaseVersionStrategy Lease Timeouts**
   - May occur with slow test execution
   - Lease duration is optimized for test scenarios

### Performance Considerations

- **NonAtomicVersionStrategy**: Fastest execution, suitable for high-throughput testing
- **TableStorageVersionStrategy**: Medium performance, strong consistency
- **BlobLeaseVersionStrategy**: Medium performance, strong consistency, no Table Storage dependency

## Test Coverage

All test classes inherit from `StoreAppendTests<T>` and provide comprehensive coverage:

- ✅ Basic append operations (NoStream, sequential appends)
- ✅ Optimistic concurrency control
- ✅ Error handling and edge cases
- ✅ Strategy-specific behavior
- ✅ Performance characteristics
- ✅ Concurrent operation handling

## Documentation

For detailed information about each versioning strategy, see:
- [VersioningStrategies.md](./VersioningStrategies.md) - Comprehensive strategy documentation
- [TestInfrastructureSummary.md](./TestInfrastructureSummary.md) - Test infrastructure details
- [README.md](./README.md) - General test project information

## Contributing

When adding new tests:

1. **Follow the existing pattern** - Use separate fixtures and test classes for each strategy
2. **Add comprehensive documentation** - Include XML comments explaining expected behavior
3. **Handle strategy-specific behavior** - Account for different consistency guarantees
4. **Test edge cases** - Include error scenarios and concurrent operations
5. **Update documentation** - Keep strategy documentation current

## Support

For issues with the test infrastructure:
- Check Docker container logs
- Verify network connectivity
- Review test output for detailed error information
- Consult the comprehensive documentation in `VersioningStrategies.md`
