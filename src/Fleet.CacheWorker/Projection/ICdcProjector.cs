using Fleet.CacheWorker.Cdc;

namespace Fleet.CacheWorker.Projection;

/// <summary>Projects one batch of CDC envelopes of a single topic into the vehicle view.</summary>
public interface ICdcProjector<TRow>
    where TRow : class
{
    /// <summary>Stream name used for logs and metric tags.</summary>
    string Stream { get; }

    /// <summary>Must be idempotent: the same batch may be delivered again after a crash or rebalance.</summary>
    Task<ProjectionResult> ProjectAsync(IReadOnlyList<DebeziumEnvelope<TRow>> batch, CancellationToken cancellationToken);
}

/// <param name="Received">Envelopes in the batch.</param>
/// <param name="Written">Conditional writes sent to Valkey after coalescing.</param>
/// <param name="Applied">Writes that won the version check.</param>
public readonly record struct ProjectionResult(int Received, int Written, int Applied)
{
    /// <summary>Writes rejected as stale or duplicate (the idempotency guard at work).</summary>
    public int Stale => Written - Applied;
}
