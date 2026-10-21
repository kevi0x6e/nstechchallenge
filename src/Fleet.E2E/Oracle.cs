using Fleet.Contracts;
using Fleet.Scenario;
using Npgsql;
using F = Fleet.Contracts.VehicleViewSchema;

namespace Fleet.E2E;

/// <summary>
/// Computes the view the cache *must* converge to, straight from the source of truth.
/// Only the test harness reads the database — never the worker.
/// </summary>
internal static class Oracle
{
    public static async Task<Dictionary<string, Dictionary<string, string>>> ExpectedViewAsync(
        NpgsqlDataSource db, ScenarioPlan plan, CancellationToken ct)
    {
        var expected = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
        Dictionary<string, string> For(long id) =>
            expected.TryGetValue(F.Key(id), out var h) ? h : expected[F.Key(id)] = new(StringComparer.Ordinal);

        var alive = new HashSet<long>();
        await using (var cmd = db.CreateCommand("SELECT id, state, state_updated_at FROM vehicles"))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                var id = reader.GetInt64(0);
                alive.Add(id);
                var h = For(id);
                h[F.State] = reader.GetString(1);
                h[F.StateUpdatedAt] = F.FormatTimestamp(reader.GetFieldValue<DateTimeOffset>(2));
                h[F.Deleted] = "0";
            }
        }

        // Vehicles that existed at some point but have no row now must be tombstoned.
        foreach (var id in plan.EverCreatedVehicleIds.Where(id => !alive.Contains(id)))
        {
            For(id)[F.Deleted] = "1";
        }

        const string latestPositions = """
            SELECT DISTINCT ON (vehicle_id) vehicle_id, id, lat, lng, recorded_at
            FROM positions
            ORDER BY vehicle_id, recorded_at DESC, id DESC
            """;
        await using (var cmd = db.CreateCommand(latestPositions))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                var h = For(reader.GetInt64(0));
                var positionId = reader.GetInt64(1);
                var recordedAt = reader.GetFieldValue<DateTimeOffset>(4);
                h[F.PositionId] = positionId.ToString(System.Globalization.CultureInfo.InvariantCulture);
                h[F.Lat] = F.FormatCoordinate(reader.GetDouble(2));
                h[F.Lng] = F.FormatCoordinate(reader.GetDouble(3));
                h[F.RecordedAt] = F.FormatTimestamp(recordedAt);
                h[F.PositionVersion] = VersionToken.FromPosition(recordedAt, positionId);
            }
        }

        return expected;
    }

    /// <summary>Field-by-field comparison. stateVersion (an LSN) has no DB counterpart and is checked by the replay digest instead.</summary>
    public static List<string> Diff(
        Dictionary<string, Dictionary<string, string>> expected,
        Dictionary<string, Dictionary<string, string>> actual)
    {
        var diffs = new List<string>();
        foreach (var key in expected.Keys.Union(actual.Keys).Order(StringComparer.Ordinal))
        {
            if (!actual.TryGetValue(key, out var a))
            {
                diffs.Add($"{key}: missing in cache");
                continue;
            }

            if (!expected.TryGetValue(key, out var e))
            {
                diffs.Add($"{key}: unexpected in cache");
                continue;
            }

            foreach (var field in e.Keys.Union(a.Keys).Where(f => f != F.StateVersion).Order(StringComparer.Ordinal))
            {
                var ev = e.GetValueOrDefault(field);
                var av = a.GetValueOrDefault(field);
                if (ev != av)
                {
                    diffs.Add($"{key}.{field}: expected '{ev ?? "<absent>"}' got '{av ?? "<absent>"}'");
                }
            }
        }

        return diffs;
    }
}
