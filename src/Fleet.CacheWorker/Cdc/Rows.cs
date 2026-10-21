namespace Fleet.CacheWorker.Cdc;

/// <summary>Row image of <c>public.vehicles</c>.</summary>
/// <remarks>
/// Non-key columns are nullable strings on purpose: under REPLICA IDENTITY DEFAULT a delete's
/// <c>before</c> image carries only the key, and the other columns may be null or placeholders.
/// They are parsed only for create/update/read events.
/// </remarks>
public sealed record VehicleRow(long Id, string? State, string? StateUpdatedAt);

/// <summary>Row image of <c>public.positions</c> (insert-only).</summary>
public sealed record PositionRow(long Id, long VehicleId, double Lat, double Lng, DateTimeOffset RecordedAt);
