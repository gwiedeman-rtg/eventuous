// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Tests.Azure.CosmosDb.Integration;
using TUnit.Core.Interfaces;

[assembly: ParallelLimiter<Limiter>]

namespace Eventuous.Tests.Azure.CosmosDb.Integration;

/// <summary>
/// Limits parallel test execution to avoid Docker resource exhaustion
/// </summary>
public class Limiter : IParallelLimit {
    public int Limit => 4; // Run tests with limited parallelism to avoid Docker network exhaustion
}



