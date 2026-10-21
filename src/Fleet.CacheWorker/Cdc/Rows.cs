namespace Fleet.CacheWorker.Cdc;

/// <summary>Row image of <c>public.vehicles</c>. Nullable because a delete's <c>before</c> carries only the key under REPLICA IDENTITY DEFAULT.</summary>
public sealed record VehicleRow(long Id, string? State, DateTimeOffset? StateUpdatedAt);

/// <summary>Row image of <c>public.positions</c> (insert-only).</summary>
public sealed record PositionRow(long Id, long VehicleId, double Lat, double Lng, DateTimeOffset RecordedAt);
