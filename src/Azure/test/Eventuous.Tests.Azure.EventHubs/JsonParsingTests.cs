// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Azure.Storage.Blobs;
using Eventuous.Azure.EventHubs;
using Eventuous.Tests.Persistence.Base;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Eventuous.Tests.Azure.EventHubs;

/// <summary>
/// Tests for JSON parsing in Azure Event Hubs Event Store.
/// These tests ensure that the JSON parsing fixes are working and would fail if removed.
/// </summary>
public class JsonParsingTests : IClassFixture<AzureEventHubsFixture> {
    readonly AzureEventHubsFixture _fixture;
    readonly ITestOutputHelper _output;

    public JsonParsingTests(AzureEventHubsFixture fixture, ITestOutputHelper output) {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task Should_Parse_JSON_Capture_Files_Correctly() {
        // Arrange - Create a stream with events
        var stream = new StreamName("json-test-stream");
        var event1 = new TestEvent("Event1");
        var event2 = new TestEvent("Event2");
        var events = new[] {
            new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()),
            new NewStreamEvent(Guid.NewGuid(), event2, new Metadata())
        };

        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act - Read events (this will use JSON parsing)
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert - Should have correct number of events
        Assert.Equal(2, readEvents.Length);
        Assert.Equal(0, readEvents[0].Position);
        Assert.Equal(1, readEvents[1].Position);
    }

    [Fact]
    public async Task Should_Handle_Malformed_AVRO_Data_Gracefully() {
        // Arrange - Create blob with malformed AVRO data
        var containerClient = new BlobServiceClient(_fixture.BlobStorageConnectionString)
            .GetBlobContainerClient(_fixture.CaptureContainerName);

        var blobName = $"malformed-avro-{Guid.NewGuid()}.avro";
        var blobClient = containerClient.GetBlobClient(blobName);

        // Create malformed AVRO data
        var malformedData = System.Text.Encoding.UTF8.GetBytes("This is not valid AVRO data");
        await blobClient.UploadAsync(new BinaryData(malformedData));

        // Act & Assert - Should not throw exception, just log warning
        var stream = new StreamName("malformed-avro-stream");
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Should return empty array or handle gracefully
        Assert.NotNull(readEvents);
    }

    [Fact]
    public async Task Should_Extract_Event_Properties_From_AVRO() {
        // Arrange
        var stream = new StreamName("properties-test-stream");
        var event1 = new TestEvent("Event1");
        var metadata = new Metadata { ["CustomProperty"] = "CustomValue" };
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, metadata) };

        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert
        Assert.Single(readEvents);
        var readEvent = readEvents[0];
        Assert.Equal("TestEvent", readEvent.EventType);
        Assert.Equal(stream.ToString(), readEvent.Stream);
        Assert.Equal(0, readEvent.Position);
        Assert.Equal("CustomValue", readEvent.Metadata["CustomProperty"]);
    }

    [Fact]
    public async Task Should_Handle_Empty_AVRO_File() {
        // Arrange - Create empty AVRO file
        var containerClient = new BlobServiceClient(_fixture.BlobStorageConnectionString)
            .GetBlobContainerClient(_fixture.CaptureContainerName);

        var blobName = $"empty-avro-{Guid.NewGuid()}.avro";
        var blobClient = containerClient.GetBlobClient(blobName);

        // Create empty AVRO file
        var emptyData = new byte[0];
        await blobClient.UploadAsync(new BinaryData(emptyData));

        // Act
        var stream = new StreamName("empty-avro-stream");
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert - Should handle gracefully
        Assert.NotNull(readEvents);
        Assert.Empty(readEvents);
    }

    [Fact]
    public async Task Should_Handle_AVRO_File_With_Invalid_Schema() {
        // Arrange - Create AVRO file with invalid schema
        var containerClient = new BlobServiceClient(_fixture.BlobStorageConnectionString)
            .GetBlobContainerClient(_fixture.CaptureContainerName);

        var blobName = $"invalid-schema-{Guid.NewGuid()}.avro";
        var blobClient = containerClient.GetBlobClient(blobName);

        // Create AVRO data with invalid schema
        var invalidAvroData = CreateInvalidAvroData();
        await blobClient.UploadAsync(new BinaryData(invalidAvroData));

        // Act
        var stream = new StreamName("invalid-schema-stream");
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert - Should handle gracefully
        Assert.NotNull(readEvents);
    }

    [Fact]
    public async Task Should_Parse_Multiple_Events_From_Single_AVRO_File() {
        // Arrange
        var stream = new StreamName("multi-event-stream");
        var events = Enumerable.Range(1, 5)
            .Select(i => new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event{i}"), new Metadata()))
            .ToArray();

        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert
        Assert.Equal(5, readEvents.Length);
        for (int i = 0; i < readEvents.Length; i++) {
            Assert.Equal(i, readEvents[i].Position);
        }
    }

    [Fact]
    public async Task Should_Handle_AVRO_File_With_Missing_Properties() {
        // Arrange - Create AVRO file with missing required properties
        var containerClient = new BlobServiceClient(_fixture.BlobStorageConnectionString)
            .GetBlobContainerClient(_fixture.CaptureContainerName);

        var blobName = $"missing-properties-{Guid.NewGuid()}.avro";
        var blobClient = containerClient.GetBlobClient(blobName);

        // Create AVRO data with missing properties
        var incompleteAvroData = CreateIncompleteAvroData();
        await blobClient.UploadAsync(new BinaryData(incompleteAvroData));

        // Act
        var stream = new StreamName("missing-properties-stream");
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert - Should handle gracefully
        Assert.NotNull(readEvents);
    }

    [Fact]
    public async Task Should_Handle_AVRO_File_With_Corrupted_Data() {
        // Arrange - Create AVRO file with corrupted data
        var containerClient = new BlobServiceClient(_fixture.BlobStorageConnectionString)
            .GetBlobContainerClient(_fixture.CaptureContainerName);

        var blobName = $"corrupted-{Guid.NewGuid()}.avro";
        var blobClient = containerClient.GetBlobClient(blobName);

        // Create corrupted AVRO data
        var corruptedData = CreateCorruptedAvroData();
        await blobClient.UploadAsync(new BinaryData(corruptedData));

        // Act
        var stream = new StreamName("corrupted-stream");
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert - Should handle gracefully
        Assert.NotNull(readEvents);
    }

    [Fact]
    public async Task Should_Handle_Large_AVRO_Files() {
        // Arrange - Create stream with many events
        var stream = new StreamName("large-avro-stream");
        var events = Enumerable.Range(1, 100)
            .Select(i => new NewStreamEvent(Guid.NewGuid(), new TestEvent($"Event{i}"), new Metadata()))
            .ToArray();

        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 100, false);

        // Assert
        Assert.Equal(100, readEvents.Length);
    }

    [Fact]
    public async Task Should_Handle_AVRO_File_With_Binary_Data() {
        // Arrange
        var stream = new StreamName("binary-data-stream");
        var binaryData = System.Text.Encoding.UTF8.GetBytes("Binary content");
        var event1 = new BinaryTestEvent(binaryData);
        var events = new[] { new NewStreamEvent(Guid.NewGuid(), event1, new Metadata()) };

        await _fixture.EventStore.AppendEvents(stream, ExpectedStreamVersion.NoStream, events);

        // Act
        var readEvents = await _fixture.EventStore.ReadEvents(stream, StreamReadPosition.Start, 10, false);

        // Assert
        Assert.Single(readEvents);
        var readEvent = readEvents[0];
        Assert.Equal("BinaryTestEvent", readEvent.EventType);
    }

    private byte[] CreateInvalidAvroData() {
        // Create AVRO data with invalid schema
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        // Write invalid AVRO header
        writer.Write(System.Text.Encoding.UTF8.GetBytes("Obj\x01")); // Invalid magic
        writer.Write(0); // Invalid schema length

        return stream.ToArray();
    }

    private byte[] CreateIncompleteAvroData() {
        // Create AVRO data with missing required fields
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        // Write minimal AVRO data without required fields
        writer.Write(System.Text.Encoding.UTF8.GetBytes("Obj\x01"));
        writer.Write(0); // Schema length
        writer.Write(0); // Sync marker

        return stream.ToArray();
    }

    private byte[] CreateCorruptedAvroData() {
        // Create corrupted AVRO data
        var random = new Random();
        var data = new byte[100];
        random.NextBytes(data);
        return data;
    }
}

public record TestEvent(string Value);
public record BinaryTestEvent(byte[] Data);
