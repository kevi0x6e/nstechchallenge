using System.Globalization;
using Fleet.CacheWorker.Cdc;
using Fleet.Contracts;

namespace Fleet.CacheWorker.Projection;

/// <summary>Translates Debezium envelopes into versioned view events. Returns null for events the view ignores.</summary>
public static class CdcMapper
{
    public static VehicleStateChange? ToStateChange(DebeziumEnvelope<VehicleRow> envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        var version = VersionToken.FromLsn(envelope.Source.Lsn);

        return envelope.Op switch
        {
            // Snapshot reads and live inserts/updates are the same thing for a read model:
            // the LSN guard makes the snapshot -> streaming overlap harmless.
            DebeziumOp.Create or DebeziumOp.Update or DebeziumOp.Read when envelope.After is { } after =>
                new VehicleStateChange(after.Id, version, Deleted: false, after.State, ParseTimestamp(after.StateUpdatedAt)),
            DebeziumOp.Delete when envelope.Before is { } before =>
                new VehicleStateChange(before.Id, version, Deleted: true, State: null, StateUpdatedAt: null),
            _ => null,
        };
    }

    private static DateTimeOffset? ParseTimestamp(string? value) =>
        string.IsNullOrEmpty(value) ? null : DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);

    public static PositionObserved? ToPosition(DebeziumEnvelope<PositionRow> envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        // Positions are insert-only; anything else (e.g. a manual delete) is not part of the view.
        if (envelope.Op is not (DebeziumOp.Create or DebeziumOp.Read) || envelope.After is not { } row)
        {
            return null;
        }

        return new PositionObserved(
            row.VehicleId,
            VersionToken.FromPosition(row.RecordedAt, row.Id),
            row.Id,
            row.Lat,
            row.Lng,
            row.RecordedAt);
    }
}
