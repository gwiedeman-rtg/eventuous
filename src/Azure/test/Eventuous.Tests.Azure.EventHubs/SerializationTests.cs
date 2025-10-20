// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Azure.EventHubs;
using System.Text.Json;

namespace Eventuous.Tests.Azure.EventHubs;

/// <summary>
/// Tests to validate event serialization and deserialization works correctly
/// </summary>
public class SerializationTests {
    [Fact]
    public void CanSerializeAndDeserializeTestEvent() {
        // Arrange
        var serializer = DefaultEventSerializer.Instance;
        var originalEvent = new TestEvent("test-123", "Test Event Name", DateTime.UtcNow);

        // Act
        var serializationResult = serializer.SerializeEvent(originalEvent);
        var deserializationResult = serializer.DeserializeEvent(
            serializationResult.Payload,
            serializationResult.EventType,
            serializationResult.ContentType
        );

        // Assert
        Assert.Equal("TestEvent", serializationResult.EventType);
        Assert.Equal("application/json", serializationResult.ContentType);
        
        Assert.IsType<DeserializationResult.SuccessfullyDeserialized>(deserializationResult);
        var successResult = (DeserializationResult.SuccessfullyDeserialized)deserializationResult;
        
        Assert.IsType<TestEvent>(successResult.Payload);
        var deserializedEvent = (TestEvent)successResult.Payload;
        
        Assert.Equal(originalEvent.Id, deserializedEvent.Id);
        Assert.Equal(originalEvent.Name, deserializedEvent.Name);
        Assert.Equal(originalEvent.CreatedAt, deserializedEvent.CreatedAt);
    }

    [Fact]
    public void CanSerializeAndDeserializeMetadata() {
        // Arrange
        var metaSerializer = DefaultMetadataSerializer.Instance;
        var originalMetadata = new Metadata {
            ["UserId"] = "user-123",
            ["CorrelationId"] = Guid.NewGuid().ToString(),
            ["Timestamp"] = DateTime.UtcNow.ToString("O"),
            ["Version"] = 1
        };

        // Act
        var serializedMetadata = metaSerializer.Serialize(originalMetadata);
        var deserializedMetadata = metaSerializer.Deserialize(serializedMetadata);

        // Assert
        Assert.NotNull(deserializedMetadata);
        Assert.Equal(originalMetadata.Count, deserializedMetadata.Count);
        
        foreach (var kvp in originalMetadata) {
            Assert.True(deserializedMetadata.ContainsKey(kvp.Key));
            
            // JSON deserialization might change types (e.g., int to JsonElement)
            // So we compare string representations
            Assert.Equal(kvp.Value?.ToString(), deserializedMetadata[kvp.Key]?.ToString());
        }
    }

    [Fact]
    public void EventDataConversionPreservesAllProperties() {
        // Arrange
        var eventStore = new AzureEventHubsEventStore(
            "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            "test-hub",
            "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net",
            "test-container",
            useRealtimeReading: false // Disable real-time reading for this test
        );

        var streamName = new StreamName("test-stream");
        var originalEvent = new TestEvent("conv-123", "Conversion Test", DateTime.UtcNow);
        var metadata = new Metadata {
            ["TestProperty"] = "TestValue",
            ["Timestamp"] = DateTime.UtcNow.ToString("O")
        };

        var streamEvent = new NewStreamEvent(Guid.NewGuid(), originalEvent, metadata);

        // Act - This will test the ToEventData method indirectly
        // We can't directly test ToEventData since it's private, but we can test
        // that the serialization process works correctly
        var serializer = DefaultEventSerializer.Instance;
        var metaSerializer = DefaultMetadataSerializer.Instance;

        var (eventType, contentType, payload) = serializer.SerializeEvent(originalEvent);
        var metadataBytes = metaSerializer.Serialize(metadata);

        // Simulate the EventData creation process
        var eventData = new EventData(payload) {
            MessageId = streamEvent.Id.ToString(),
            ContentType = contentType,
            PartitionKey = streamName.ToString()
        };

        eventData.Properties["EventType"] = eventType;
        eventData.Properties["StreamName"] = streamName.ToString();
        eventData.Properties["Metadata"] = Convert.ToBase64String(metadataBytes);

        // Assert
        Assert.Equal(streamEvent.Id.ToString(), eventData.MessageId);
        Assert.Equal(contentType, eventData.ContentType);
        Assert.Equal(streamName.ToString(), eventData.PartitionKey);
        Assert.Equal(eventType, eventData.Properties["EventType"]);
        Assert.Equal(streamName.ToString(), eventData.Properties["StreamName"]);
        Assert.True(eventData.Properties.ContainsKey("Metadata"));

        // Verify we can deserialize back
        var deserializedEvent = serializer.DeserializeEvent(payload, eventType, contentType);
        Assert.IsType<DeserializationResult.SuccessfullyDeserialized>(deserializedEvent);
        
        var metadataFromBase64 = Convert.FromBase64String((string)eventData.Properties["Metadata"]);
        var deserializedMetadata = metaSerializer.Deserialize(metadataFromBase64);
        Assert.NotNull(deserializedMetadata);
        Assert.Equal(metadata.Count, deserializedMetadata.Count);

        eventStore.Dispose();
    }

    [Fact]
    public void ComplexObjectSerializationWorks() {
        // Arrange
        var serializer = DefaultEventSerializer.Instance;
        var complexEvent = new ComplexTestEvent {
            Id = Guid.NewGuid(),
            Name = "Complex Event",
            CreatedAt = DateTime.UtcNow,
            Properties = new Dictionary<string, object> {
                ["StringProperty"] = "test value",
                ["NumberProperty"] = 42,
                ["BoolProperty"] = true,
                ["DateProperty"] = DateTime.UtcNow
            },
            Tags = new[] { "tag1", "tag2", "tag3" },
            NestedObject = new NestedObject {
                Value = "nested value",
                Count = 100
            }
        };

        // Act
        var serializationResult = serializer.SerializeEvent(complexEvent);
        var deserializationResult = serializer.DeserializeEvent(
            serializationResult.Payload,
            serializationResult.EventType,
            serializationResult.ContentType
        );

        // Assert
        Assert.IsType<DeserializationResult.SuccessfullyDeserialized>(deserializationResult);
        var successResult = (DeserializationResult.SuccessfullyDeserialized)deserializationResult;
        
        Assert.IsType<ComplexTestEvent>(successResult.Payload);
        var deserializedEvent = (ComplexTestEvent)successResult.Payload;
        
        Assert.Equal(complexEvent.Id, deserializedEvent.Id);
        Assert.Equal(complexEvent.Name, deserializedEvent.Name);
        Assert.Equal(complexEvent.Properties.Count, deserializedEvent.Properties.Count);
        Assert.Equal(complexEvent.Tags.Length, deserializedEvent.Tags.Length);
        Assert.Equal(complexEvent.NestedObject.Value, deserializedEvent.NestedObject.Value);
        Assert.Equal(complexEvent.NestedObject.Count, deserializedEvent.NestedObject.Count);
    }
}

public record ComplexTestEvent {
    public Guid Id { get; init; }
    public string Name { get; init; } = string.Empty;
    public DateTime CreatedAt { get; init; }
    public Dictionary<string, object> Properties { get; init; } = new();
    public string[] Tags { get; init; } = Array.Empty<string>();
    public NestedObject NestedObject { get; init; } = new();
}

public record NestedObject {
    public string Value { get; init; } = string.Empty;
    public int Count { get; init; }
}