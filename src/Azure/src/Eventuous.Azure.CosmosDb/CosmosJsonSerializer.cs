// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.IO;
using System.Text;
using System.Text.Json;
using Microsoft.Azure.Cosmos;

namespace Eventuous.Azure.CosmosDb;

/// <summary>
/// Custom CosmosSerializer that respects JsonPropertyName attributes
/// </summary>
public class CosmosJsonSerializer : CosmosSerializer {
    readonly JsonSerializerOptions _options;

    public CosmosJsonSerializer(JsonSerializerOptions? options = null) {
        _options = options ?? new JsonSerializerOptions {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            // Ensure JsonPropertyName attributes are respected
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.Never
        };
    }

    public override T FromStream<T>(Stream stream) {
        if (stream is null || stream.CanSeek && stream.Length == 0) {
            return default!;
        }

        if (typeof(Stream).IsAssignableFrom(typeof(T))) {
            return (T)(object)stream;
        }

        using var reader = new StreamReader(stream);
        var json = reader.ReadToEnd();
        return JsonSerializer.Deserialize<T>(json, _options)!;
    }

    public override Stream ToStream<T>(T input) {
        var stream = new MemoryStream();
        JsonSerializer.Serialize(new Utf8JsonWriter(stream), input, _options);
        stream.Position = 0;
        return stream;
    }
}

