// Copyright (C) Eventuous HQ OÜ. All rights reserved
// Licensed under the Apache License, Version 2.0.

using Eventuous.Tests.Persistence.Base.Store;
using TUnit.Core;
using TUnit.Core.Helpers;

// ReSharper disable UnusedType.Global

namespace Eventuous.Tests.Azure.EventHubs.Integration.Store;

[InheritsTests]
[ClassDataSource<StoreFixture>]
[ParallelLimiter<DefaultParallelLimit>]
public class Append(StoreFixture fixture) : StoreAppendTests<StoreFixture>(fixture);

[InheritsTests]
[ClassDataSource<StoreFixture>]
[ParallelLimiter<DefaultParallelLimit>]
public class Read(StoreFixture fixture) : StoreReadTests<StoreFixture>(fixture);

[InheritsTests]
[ClassDataSource<StoreFixture>]
[ParallelLimiter<DefaultParallelLimit>]
public class OtherMethods(StoreFixture fixture) : StoreOtherOpsTests<StoreFixture>(fixture);