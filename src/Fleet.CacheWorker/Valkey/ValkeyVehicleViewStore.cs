using System.Globalization;
using Fleet.CacheWorker.Projection;
using Fleet.Contracts;
using StackExchange.Redis;

namespace Fleet.CacheWorker.Valkey;

/// <summary>
/// Sends one conditional script call per vehicle and awaits them together: the multiplexer
/// pipelines them over a single connection, so a batch costs ~1 network round-trip.
/// StackExchange.Redis caches the script SHA and uses EVALSHA (falling back to EVAL on NOSCRIPT).
/// </summary>
public sealed class ValkeyVehicleViewStore(IConnectionMultiplexer connection) : IVehicleViewStore
{
    public Task<int> ApplyStateChangesAsync(IReadOnlyCollection<VehicleStateChange> changes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(changes);
        return ExecuteAsync(changes, LuaScripts.ApplyState, change =>
        [
            change.Version,
            change.Deleted ? "1" : "0",
            change.State ?? string.Empty,
            change.StateUpdatedAt is { } at ? VehicleViewSchema.FormatTimestamp(at) : string.Empty,
        ]);
    }

    public Task<int> ApplyPositionsAsync(IReadOnlyCollection<PositionObserved> positions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(positions);
        return ExecuteAsync(positions, LuaScripts.ApplyPosition, position =>
        [
            position.Version,
            position.PositionId.ToString(CultureInfo.InvariantCulture),
            VehicleViewSchema.FormatCoordinate(position.Lat),
            VehicleViewSchema.FormatCoordinate(position.Lng),
            VehicleViewSchema.FormatTimestamp(position.RecordedAt),
        ]);
    }

    private async Task<int> ExecuteAsync<T>(IReadOnlyCollection<T> events, string script, Func<T, RedisValue[]> args)
        where T : IVehicleEvent
    {
        if (events.Count == 0)
        {
            return 0;
        }

        var db = connection.GetDatabase();
        var pending = new Task<RedisResult>[events.Count];
        var i = 0;
        foreach (var e in events)
        {
            pending[i++] = db.ScriptEvaluateAsync(script, [VehicleViewSchema.Key(e.VehicleId)], args(e));
        }

        var results = await Task.WhenAll(pending);
        return results.Count(r => (int)r == 1);
    }
}
