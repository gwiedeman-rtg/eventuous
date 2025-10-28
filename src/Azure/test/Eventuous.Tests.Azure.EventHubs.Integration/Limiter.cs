using Eventuous.Tests.Azure.EventHubs.Integration;
using TUnit.Core.Interfaces;

[assembly: ParallelLimiter<Limiter>]

namespace Eventuous.Tests.Azure.EventHubs.Integration;

public class Limiter : IParallelLimit {
    public int Limit => 4; // Run tests sequentially to avoid Docker network exhaustion
}
