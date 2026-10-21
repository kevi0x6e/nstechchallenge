using Fleet.CacheWorker.Cdc;

namespace Fleet.CacheWorker.Tests;

/// <summary>Terse builders for Debezium envelopes used by the behavior tests.</summary>
internal static class Cdc
{
    public static readonly DateTimeOffset T0 = new(2026, 10, 20, 10, 0, 0, TimeSpan.Zero);

    public static DebeziumEnvelope<VehicleRow> Snapshot(long id, long lsn, string state, int atSeconds = 0) =>
        new(null, new VehicleRow(id, state, T0.AddSeconds(atSeconds)), new DebeziumSource(lsn, 0, "vehicles"), DebeziumOp.Read);

    public static DebeziumEnvelope<VehicleRow> Insert(long id, long lsn, string state, int atSeconds = 0) =>
        new(null, new VehicleRow(id, state, T0.AddSeconds(atSeconds)), new DebeziumSource(lsn, 0, "vehicles"), DebeziumOp.Create);

    public static DebeziumEnvelope<VehicleRow> Update(long id, long lsn, string state, int atSeconds = 0) =>
        new(new VehicleRow(id, null, null), new VehicleRow(id, state, T0.AddSeconds(atSeconds)), new DebeziumSource(lsn, 0, "vehicles"), DebeziumOp.Update);

    public static DebeziumEnvelope<VehicleRow> Delete(long id, long lsn) =>
        new(new VehicleRow(id, null, null), null, new DebeziumSource(lsn, 0, "vehicles"), DebeziumOp.Delete);

    public static DebeziumEnvelope<PositionRow> Position(long vehicleId, long positionId, int recordedAtSeconds, double lat = -23.5, double lng = -46.6) =>
        new(null, new PositionRow(positionId, vehicleId, lat, lng, T0.AddSeconds(recordedAtSeconds)), new DebeziumSource(positionId * 10, 0, "positions"), DebeziumOp.Create);
}
