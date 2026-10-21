namespace Fleet.Scenario;

/// <summary>A single write to the source database. Writers only ever touch PostgreSQL.</summary>
public abstract record VehicleOp(long VehicleId);

public sealed record InsertVehicle(long VehicleId, string Plate, string State, DateTimeOffset StateUpdatedAt) : VehicleOp(VehicleId);

/// <param name="IsLate">The business timestamp is older than the vehicle's current one (injected disorder).</param>
public sealed record UpdateVehicleState(long VehicleId, string State, DateTimeOffset StateUpdatedAt, bool IsLate) : VehicleOp(VehicleId);

public sealed record DeleteVehicle(long VehicleId) : VehicleOp(VehicleId);

/// <param name="IsLate">recordedAt is older than the vehicle's latest position (out-of-order telemetry).</param>
/// <param name="IsDuplicate">Same payload inserted again (at-least-once device upload).</param>
public sealed record InsertPosition(long VehicleId, double Lat, double Lng, DateTimeOffset RecordedAt, bool IsLate, bool IsDuplicate);

public static class VehicleStates
{
    public const string Registered = "REGISTERED";
    public const string Active = "ACTIVE";
    public const string Idle = "IDLE";
    public const string Maintenance = "MAINTENANCE";
    public const string Decommissioned = "DECOMMISSIONED";

    /// <summary>Allowed lifecycle transitions; DECOMMISSIONED is terminal.</summary>
    public static readonly IReadOnlyDictionary<string, string[]> Transitions = new Dictionary<string, string[]>
    {
        [Registered] = [Active],
        [Active] = [Idle, Maintenance, Decommissioned],
        [Idle] = [Active, Maintenance, Decommissioned],
        [Maintenance] = [Active, Idle],
        [Decommissioned] = [],
    };
}
