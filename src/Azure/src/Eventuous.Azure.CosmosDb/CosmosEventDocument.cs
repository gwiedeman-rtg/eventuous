// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Text.Json.Serialization;

namespace Eventuous.Azure.CosmosDb;

/// <summary>
/// Document structure for events stored in Cosmos DB
/// </summary>
internal class CosmosEventDocument {
    [JsonPropertyName("id")]
    public string Id { get; set; } = null!;

    [JsonPropertyName("streamId")]
    public string StreamId { get; set; } = null!;

    [JsonPropertyName("messageType")]
    public string MessageType { get; set; } = null!;

    [JsonPropertyName("contentType")]
    public string ContentType { get; set; } = null!;

    [JsonPropertyName("jsonData")]
    public string JsonData { get; set; } = null!;

    [JsonPropertyName("jsonMetadata")]
    public string? JsonMetadata { get; set; }

    [JsonPropertyName("streamPosition")]
    public long StreamPosition { get; set; }

    [JsonPropertyName("globalPosition")]
    public ulong GlobalPosition { get; set; }

    [JsonPropertyName("created")]
    public DateTime Created { get; set; }
}


