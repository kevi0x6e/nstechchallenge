namespace Fleet.CacheWorker.Projection;

/// <summary>A change to one field group of a vehicle view, carrying its order-preserving version token.</summary>
public interface IVehicleEvent
{
    long VehicleId { get; }
    string Version { get; }
}

/// <summary>State-group change. <see cref="Deleted"/> = true is the tombstone.</summary>
public sealed record VehicleStateChange(
    long VehicleId,
    string Version,
    bool Deleted,
    string? State,
    DateTimeOffset? StateUpdatedAt) : IVehicleEvent;

/// <summary>Position-group change.</summary>
public sealed record PositionObserved(
    long VehicleId,
    string Version,
    long PositionId,
    double Lat,
    double Lng,
    DateTimeOffset RecordedAt) : IVehicleEvent;
