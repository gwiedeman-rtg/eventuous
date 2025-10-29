# Version Strategies for Azure Event Hubs

The Azure Event Hubs Event Store supports multiple versioning strategies for optimistic concurrency control. Version strategies are now fully injectable and extensible, allowing you to create custom implementations.

## Built-in Strategies

### 1. **TableStorageVersionStrategy** (Atomic)
- Uses Azure Table Storage with ETag-based conditional updates
- Provides strong consistency guarantees
- Requires `TableStorageConnectionString`
- Best for production scenarios requiring strict consistency

### 2. **BlobLeaseVersionStrategy** (Atomic)
- Uses Azure Blob Lease for distributed locking
- Provides strong consistency without requiring Table Storage
- Uses blob leases to prevent concurrent modifications
- Requires `BlobServiceClient`
- Best when you want atomic versioning but don't have Table Storage

### 3. **NonAtomicVersionStrategy** (Non-Atomic)
- Uses `GetCurrentStreamVersion()` to determine stream versions
- Fastest but subject to race conditions
- May skip version validation if version cannot be determined
- Best for scenarios where performance is critical and eventual consistency is acceptable

## Using Built-in Strategies

### Via Configuration (Recommended)

```csharp
services.AddAzureEventHubsEventStore(options => {
    options.EventHubConnectionString = "...";
    options.EventHubName = "my-hub";
    options.BlobStorageConnectionString = "...";
    options.CaptureContainerName = "events";
    options.TableStorageConnectionString = "..."; // For TableStorageVersionStrategy
    options.EnableAtomicVersioning = true; // Use atomic strategy
});
```

The strategy is automatically selected:
- If `EnableAtomicVersioning = true` and `TableStorageConnectionString` is provided → `TableStorageVersionStrategy`
- If `EnableAtomicVersioning = true` and no `TableStorageConnectionString` → `BlobLeaseVersionStrategy`
- If `EnableAtomicVersioning = false` → `NonAtomicVersionStrategy`

### Via Direct Injection

```csharp
// Register the strategy first
services.AddSingleton<IStreamVersionStrategy>(serviceProvider => {
    var tableServiceClient = new TableServiceClient("...");
    var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
    return new TableStorageVersionStrategy(tableServiceClient, loggerFactory.CreateLogger<TableStorageVersionStrategy>());
});

// Then register the event store
services.AddAzureEventHubsEventStore(options => {
    // ... configure options
});
```

## Creating Custom Strategies

### Option 1: Direct Strategy Implementation

Implement `IStreamVersionStrategy`:

```csharp
public class MyCustomVersionStrategy : IStreamVersionStrategy {
    private readonly IMyService _myService;
    private readonly ILogger<MyCustomVersionStrategy>? _logger;

    public MyCustomVersionStrategy(IMyService myService, ILogger<MyCustomVersionStrategy>? logger = null) {
        _myService = myService;
        _logger = logger;
    }

    public async Task<long?> GetVersion(StreamName stream, CancellationToken cancellationToken) {
        // Your implementation to get current version
        return await _myService.GetStreamVersionAsync(stream.ToString(), cancellationToken);
    }

    public async Task<long> IncrementVersion(
        StreamName stream,
        long expectedVersion,
        int eventCount,
        CancellationToken cancellationToken
    ) {
        // Your implementation to atomically increment version
        // Throw AppendToStreamException if expectedVersion doesn't match
        var newVersion = await _myService.IncrementVersionAsync(
            stream.ToString(),
            expectedVersion,
            eventCount,
            cancellationToken
        );
        return newVersion;
    }
}
```

Register your custom strategy:

```csharp
// Register dependencies
services.AddSingleton<IMyService, MyService>();

// Register custom version strategy
services.RegisterVersionStrategy<MyCustomVersionStrategy>();

// Or register an instance
services.RegisterVersionStrategy(new MyCustomVersionStrategy(new MyService()));

// Register event store (will use your custom strategy)
services.AddAzureEventHubsEventStore(options => {
    // ... configure options
});
```

### Option 2: Custom Factory

Implement `IVersionStrategyFactory` for more control:

```csharp
public class MyVersionStrategyFactory : IVersionStrategyFactory {
    private readonly IMyService _myService;
    private readonly ILoggerFactory? _loggerFactory;

    public MyVersionStrategyFactory(IMyService myService, ILoggerFactory? loggerFactory = null) {
        _myService = myService;
        _loggerFactory = loggerFactory;
    }

    public IStreamVersionStrategy CreateVersionStrategy(VersionStrategyContext context) {
        // You can inspect context and create different strategies based on configuration
        if (context.EnableAtomicVersioning) {
            return new MyAtomicVersionStrategy(_myService, _loggerFactory?.CreateLogger<MyAtomicVersionStrategy>());
        }

        // Note: NonAtomicVersionStrategy requires EventStore instance via context.EventStore
        if (context.EventStore == null) {
            throw new InvalidOperationException("Non-atomic strategies require EventStore instance");
        }

        return new NonAtomicVersionStrategy(
            context.EventStore,
            _loggerFactory?.CreateLogger<NonAtomicVersionStrategy>()
        );
    }
}
```

Register your factory:

```csharp
services.RegisterVersionStrategyFactory<MyVersionStrategyFactory>();

// Or register an instance
services.RegisterVersionStrategyFactory(new MyVersionStrategyFactory(new MyService()));
```

## Strategy Selection Priority

The event store selects a version strategy in this order:

1. **Injected `IStreamVersionStrategy`** - If registered in DI container, this takes highest priority
2. **Registered `IVersionStrategyFactory`** - If a factory is registered, it's used to create the strategy
3. **Configuration-based creation** - Falls back to creating built-in strategies based on `EnableAtomicVersioning` and connection strings

## Best Practices

1. **For Production**: Use atomic strategies (`TableStorageVersionStrategy` or `BlobLeaseVersionStrategy`)
2. **For Testing**: Non-atomic strategies are fine if you can tolerate race conditions
3. **Custom Strategies**: Ensure your custom strategy properly throws `AppendToStreamException` on version mismatches
4. **Injection**: Prefer injecting strategies via DI rather than configuration for maximum flexibility

## Example: Redis-Based Version Strategy

```csharp
public class RedisVersionStrategy : IStreamVersionStrategy {
    private readonly IDatabase _redis;
    private readonly ILogger<RedisVersionStrategy>? _logger;

    public RedisVersionStrategy(IDatabase redis, ILogger<RedisVersionStrategy>? logger = null) {
        _redis = redis;
        _logger = logger;
    }

    public async Task<long?> GetVersion(StreamName stream, CancellationToken cancellationToken) {
        var key = $"stream:version:{stream}";
        var value = await _redis.StringGetAsync(key);
        return value.HasValue ? (long)value : null;
    }

    public async Task<long> IncrementVersion(
        StreamName stream,
        long expectedVersion,
        int eventCount,
        CancellationToken cancellationToken
    ) {
        var key = $"stream:version:{stream}";
        var transaction = _redis.CreateTransaction();

        // Use Redis WATCH/MULTI/EXEC for atomicity
        transaction.AddCondition(Condition.StringEqual(key, expectedVersion == -1 ? RedisValue.Null : expectedVersion));

        var newVersion = expectedVersion == -1 ? eventCount - 1 : expectedVersion + eventCount;
        transaction.StringSetAsync(key, newVersion);

        if (!await transaction.ExecuteAsync()) {
            throw new AppendToStreamException(stream, new InvalidOperationException(
                $"WrongExpectedVersion {expectedVersion}, concurrent modification detected"));
        }

        return newVersion;
    }
}

// Usage:
services.AddStackExchangeRedisCache(options => { /* ... */ });
services.AddSingleton<IDatabase>(sp => {
    var connection = sp.GetRequiredService<IConnectionMultiplexer>();
    return connection.GetDatabase();
});
services.RegisterVersionStrategy<RedisVersionStrategy>();
```

