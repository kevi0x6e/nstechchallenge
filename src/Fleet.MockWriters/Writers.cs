using System.Diagnostics;
using Fleet.Scenario;
using Npgsql;

namespace Fleet.MockWriters;

/// <summary>Writes position rows at a target rate, batching whatever is due into one round-trip.</summary>
internal sealed class PositionWriter(NpgsqlDataSource db)
{
    private const int MaxBatch = 200;

    public async Task WriteAsync(IReadOnlyList<InsertPosition> positions, double ratePerSecond, CancellationToken ct)
    {
        var clock = Stopwatch.StartNew();
        var written = 0;
        while (written < positions.Count)
        {
            var due = ratePerSecond <= 0 ? positions.Count : (int)(clock.Elapsed.TotalSeconds * ratePerSecond);
            var take = Math.Min(MaxBatch, Math.Min(due, positions.Count) - written);
            if (take <= 0)
            {
                await Task.Delay(5, ct);
                continue;
            }

            await using var batch = db.CreateBatch();
            foreach (var p in positions.Skip(written).Take(take))
            {
                batch.BatchCommands.Add(new NpgsqlBatchCommand(
                    "INSERT INTO positions (vehicle_id, lat, lng, recorded_at) VALUES ($1, $2, $3, $4)")
                {
                    Parameters = { new() { Value = p.VehicleId }, new() { Value = p.Lat }, new() { Value = p.Lng }, new() { Value = p.RecordedAt } },
                });
            }

            await batch.ExecuteNonQueryAsync(ct);
            written += take;
        }

        Console.WriteLine($"positions: {written} rows ({positions.Count(p => p.IsLate)} late, {positions.Count(p => p.IsDuplicate)} duplicates) in {clock.Elapsed.TotalSeconds:F1}s");
    }
}

/// <summary>Drives the vehicle lifecycle: inserts, state transitions (some with late business times) and deletes.</summary>
internal sealed class VehicleWriter(NpgsqlDataSource db)
{
    public async Task WriteAsync(IReadOnlyList<VehicleOp> ops, double ratePerSecond, CancellationToken ct)
    {
        var clock = Stopwatch.StartNew();
        for (var i = 0; i < ops.Count; i++)
        {
            if (ratePerSecond > 0)
            {
                var wait = TimeSpan.FromSeconds(i / ratePerSecond) - clock.Elapsed;
                if (wait > TimeSpan.Zero)
                {
                    await Task.Delay(wait, ct);
                }
            }

            await using var cmd = Command(ops[i]);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        Console.WriteLine(
            $"vehicles: {ops.Count} ops ({ops.OfType<InsertVehicle>().Count()} inserts, {ops.OfType<UpdateVehicleState>().Count()} updates " +
            $"[{ops.OfType<UpdateVehicleState>().Count(u => u.IsLate)} late], {ops.OfType<DeleteVehicle>().Count()} deletes) in {clock.Elapsed.TotalSeconds:F1}s");
    }

    private NpgsqlCommand Command(VehicleOp op) => op switch
    {
        InsertVehicle v => db.CreateCommand(
            "INSERT INTO vehicles (id, plate, state, state_updated_at) VALUES ($1, $2, $3, $4)",
            v.VehicleId, v.Plate, v.State, v.StateUpdatedAt),
        UpdateVehicleState u => db.CreateCommand(
            "UPDATE vehicles SET state = $2, state_updated_at = $3, updated_at = now() WHERE id = $1",
            u.VehicleId, u.State, u.StateUpdatedAt),
        DeleteVehicle d => db.CreateCommand("DELETE FROM vehicles WHERE id = $1", d.VehicleId),
        _ => throw new ArgumentOutOfRangeException(nameof(op), op, "unknown op"),
    };
}

internal static class DataSourceExtensions
{
    public static NpgsqlCommand CreateCommand(this NpgsqlDataSource db, string sql, params object[] values)
    {
        var cmd = db.CreateCommand(sql);
        foreach (var value in values)
        {
            cmd.Parameters.Add(new NpgsqlParameter { Value = value });
        }

        return cmd;
    }
}
