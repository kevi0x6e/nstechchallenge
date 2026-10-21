using System.Diagnostics.Metrics;

namespace Fleet.CacheWorker.Diagnostics;

/// <summary>OpenTelemetry-compatible counters (System.Diagnostics.Metrics), tagged by stream.</summary>
public sealed class CacheWorkerMetrics : IDisposable
{
    public const string MeterName = "Fleet.CacheWorker";

    private readonly Meter _meter;
    private readonly Counter<long> _received;
    private readonly Counter<long> _applied;
    private readonly Counter<long> _stale;
    private readonly Counter<long> _skipped;

    public CacheWorkerMetrics(IMeterFactory meterFactory)
    {
        ArgumentNullException.ThrowIfNull(meterFactory);
        _meter = meterFactory.Create(MeterName);
        _received = _meter.CreateCounter<long>("fleet.cdc.events.received");
        _applied = _meter.CreateCounter<long>("fleet.cache.writes.applied");
        _stale = _meter.CreateCounter<long>("fleet.cache.writes.stale");
        _skipped = _meter.CreateCounter<long>("fleet.cdc.events.skipped", description: "Kafka tombstones and undecodable messages");
    }

    public void RecordBatch(string stream, int received, int applied, int stale)
    {
        var tag = new KeyValuePair<string, object?>("stream", stream);
        _received.Add(received, tag);
        _applied.Add(applied, tag);
        _stale.Add(stale, tag);
    }

    public void RecordSkipped(string stream, string reason) =>
        _skipped.Add(1, new("stream", stream), new("reason", reason));

    public void Dispose() => _meter.Dispose();
}
