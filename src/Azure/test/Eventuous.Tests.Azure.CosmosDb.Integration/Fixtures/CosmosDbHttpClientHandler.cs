// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using System.Net.Http;

namespace Eventuous.Tests.Azure.CosmosDb.Integration.Fixtures;

/// <summary>
/// Custom HTTP message handler that rewrites Cosmos DB emulator requests to use the correct localhost port.
/// The emulator returns internal container IPs (like 172.17.0.3:8081) which need to be redirected
/// to localhost with the mapped port.
/// </summary>
public class CosmosDbHttpClientHandler : DelegatingHandler {
    readonly int _portNumber;

    public CosmosDbHttpClientHandler(int portNumber, HttpMessageHandler innerHandler) : base(innerHandler) {
        _portNumber = portNumber;
    }

    /// <summary>
    /// Override of the SendAsync method to allow for reconstruction of the request uri to point to the dynamic
    /// testcontainer port number. This needs to be done as otherwise it defaults back to 8081 or uses the
    /// internal container IP. If this is not done then the requests are not successful.
    /// </summary>
    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken
    ) {
        // Rewrite the URI to use localhost with the mapped port
        request.RequestUri = new Uri($"https://localhost:{_portNumber}{request.RequestUri!.PathAndQuery}");
        var response = await base.SendAsync(request, cancellationToken);
        return response;
    }
}

