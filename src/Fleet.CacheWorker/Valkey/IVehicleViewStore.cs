using Fleet.CacheWorker.Projection;

namespace Fleet.CacheWorker.Valkey;

/// <summary>Conditional, idempotent writes to the vehicle view. Never a blind SET.</summary>
public interface IVehicleViewStore
{
    /// <returns>How many changes won the version check.</returns>
    Task<int> ApplyStateChangesAsync(IReadOnlyCollection<VehicleStateChange> changes, CancellationToken cancellationToken);

    /// <returns>How many positions won the version check.</returns>
    Task<int> ApplyPositionsAsync(IReadOnlyCollection<PositionObserved> positions, CancellationToken cancellationToken);
}
