using static Fleet.Contracts.VehicleViewSchema;

namespace Fleet.CacheWorker.Valkey;

/// <summary>
/// Compare-and-set scripts. Each touches a single key (cluster-safe) and runs atomically,
/// so the version check and the write cannot interleave with another writer.
/// Versions are fixed-width strings: Lua's string comparison is numeric-order safe.
/// Return 1 = applied, 0 = stale/duplicate (no-op).
/// </summary>
internal static class LuaScripts
{
    /// <summary>KEYS[1]=vehicle key; ARGV: version, deleted(0|1), state, stateUpdatedAt.</summary>
    public const string ApplyState = $$"""
        local current = redis.call('HGET', KEYS[1], '{{StateVersion}}')
        if current and current >= ARGV[1] then return 0 end
        if ARGV[2] == '1' then
          redis.call('HDEL', KEYS[1], '{{State}}', '{{StateUpdatedAt}}')
          redis.call('HSET', KEYS[1], '{{StateVersion}}', ARGV[1], '{{Deleted}}', '1')
        else
          redis.call('HSET', KEYS[1], '{{StateVersion}}', ARGV[1], '{{Deleted}}', '0',
            '{{State}}', ARGV[3], '{{StateUpdatedAt}}', ARGV[4])
        end
        return 1
        """;

    /// <summary>KEYS[1]=vehicle key; ARGV: version, positionId, lat, lng, recordedAt.</summary>
    public const string ApplyPosition = $$"""
        local current = redis.call('HGET', KEYS[1], '{{PositionVersion}}')
        if current and current >= ARGV[1] then return 0 end
        redis.call('HSET', KEYS[1], '{{PositionVersion}}', ARGV[1], '{{PositionId}}', ARGV[2],
          '{{Lat}}', ARGV[3], '{{Lng}}', ARGV[4], '{{RecordedAt}}', ARGV[5])
        return 1
        """;
}
