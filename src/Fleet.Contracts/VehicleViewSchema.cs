using System.Globalization;

namespace Fleet.Contracts;

/// <summary>
/// The read contract of the Valkey vehicle view. Shared by the writer (cache worker)
/// and any reader (the e2e verifier, tracking UIs, alerting).
/// </summary>
/// <remarks>
/// One hash per vehicle at <c>vehicle:{id}</c>. The hash carries two independent field
/// groups, each guarded by its own version token:
/// <list type="bullet">
///   <item>state group — <see cref="State"/>, <see cref="StateUpdatedAt"/>, <see cref="Deleted"/>, versioned by <see cref="StateVersion"/> (Postgres LSN).</item>
///   <item>position group — <see cref="Lat"/>, <see cref="Lng"/>, <see cref="RecordedAt"/>, <see cref="PositionId"/>, versioned by <see cref="PositionVersion"/> (recordedAt, positionId).</item>
/// </list>
/// A hash without the state group means "position seen, vehicle not seen yet".
/// <c>deleted = 1</c> is a tombstone: readers must treat the vehicle as not found.
/// </remarks>
public static class VehicleViewSchema
{
    public const string KeyPrefix = "vehicle:";
    public const string KeyPattern = KeyPrefix + "*";

    public const string State = "state";
    public const string StateUpdatedAt = "stateUpdatedAt";
    public const string Deleted = "deleted";
    public const string StateVersion = "stateVersion";

    public const string Lat = "lat";
    public const string Lng = "lng";
    public const string RecordedAt = "recordedAt";
    public const string PositionId = "positionId";
    public const string PositionVersion = "positionVersion";

    /// <summary>ISO-8601 UTC with microsecond precision (Postgres timestamptz precision).</summary>
    public const string TimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'";

    public static string Key(long vehicleId) => KeyPrefix + vehicleId.ToString(CultureInfo.InvariantCulture);

    public static string FormatTimestamp(DateTimeOffset value) =>
        value.UtcDateTime.ToString(TimestampFormat, CultureInfo.InvariantCulture);

    public static string FormatCoordinate(double value) => value.ToString("R", CultureInfo.InvariantCulture);
}
