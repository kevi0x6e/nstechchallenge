using Fleet.CacheWorker.Cdc;
using Fleet.CacheWorker.Valkey;

namespace Fleet.CacheWorker.Projection;

public sealed class PositionProjector(IVehicleViewStore store) : ICdcProjector<PositionRow>
{
    public string Stream => "positions";

    public async Task<ProjectionResult> ProjectAsync(IReadOnlyList<DebeziumEnvelope<PositionRow>> batch, CancellationToken cancellationToken)
    {
        var positions = LatestWins.PerVehicle(batch.Select(CdcMapper.ToPosition).OfType<PositionObserved>());
        var applied = await store.ApplyPositionsAsync(positions, cancellationToken);
        return new ProjectionResult(batch.Count, positions.Count, applied);
    }
}
