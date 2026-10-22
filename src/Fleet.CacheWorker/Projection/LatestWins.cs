namespace Fleet.CacheWorker.Projection;

/// <summary>
/// In-batch coalescing: keeps only the highest-version event per vehicle.
/// Safe because the cache merge is max(version) — applying only the max of a batch
/// yields the same result as applying every event — and it cuts Valkey round-trips
/// on the high-frequency position stream.
/// </summary>
public static class LatestWins
{
    public static IReadOnlyCollection<T> PerVehicle<T>(IEnumerable<T> events)
        where T : IVehicleEvent
    {
        ArgumentNullException.ThrowIfNull(events);
        var latest = new Dictionary<long, T>();
        foreach (var candidate in events)
        {
            // Ordinal comparison matches the byte-wise string comparison done in Lua.
            if (!latest.TryGetValue(candidate.VehicleId, out var current)
                || string.CompareOrdinal(candidate.Version, current.Version) > 0)
            {
                latest[candidate.VehicleId] = candidate;
            }
        }

        return latest.Values;
    }
}
