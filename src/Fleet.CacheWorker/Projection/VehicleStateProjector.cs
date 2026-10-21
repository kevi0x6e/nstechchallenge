using Fleet.CacheWorker.Cdc;
using Fleet.CacheWorker.Valkey;

namespace Fleet.CacheWorker.Projection;

public sealed class VehicleStateProjector(IVehicleViewStore store) : ICdcProjector<VehicleRow>
{
    public string Stream => "vehicles";

    public async Task<ProjectionResult> ProjectAsync(IReadOnlyList<DebeziumEnvelope<VehicleRow>> batch, CancellationToken cancellationToken)
    {
        var changes = LatestWins.PerVehicle(batch.Select(CdcMapper.ToStateChange).OfType<VehicleStateChange>());
        var applied = await store.ApplyStateChangesAsync(changes, cancellationToken);
        return new ProjectionResult(batch.Count, changes.Count, applied);
    }
}
