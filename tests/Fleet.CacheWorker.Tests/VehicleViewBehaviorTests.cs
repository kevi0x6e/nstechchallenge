using Fleet.CacheWorker.Cdc;
using Fleet.CacheWorker.Projection;
using Fleet.CacheWorker.Valkey;
using Fleet.Contracts;
using static Fleet.CacheWorker.Tests.Cdc;
using F = Fleet.Contracts.VehicleViewSchema;

namespace Fleet.CacheWorker.Tests;

/// <summary>
/// Automates docs/test-behaviors.md (TB-xx ids) against a real Valkey,
/// going through the same projectors and Lua scripts the worker uses.
/// </summary>
[Collection(ValkeyCollection.Name)]
public sealed class VehicleViewBehaviorTests(ValkeyFixture valkey) : IAsyncLifetime
{
    private readonly VehicleStateProjector _vehicles = new(new ValkeyVehicleViewStore(valkey.Connection));
    private readonly PositionProjector _positions = new(new ValkeyVehicleViewStore(valkey.Connection));

    public Task InitializeAsync() => valkey.FlushAsync();

    public Task DisposeAsync() => Task.CompletedTask;

    private Task<ProjectionResult> Apply(params DebeziumEnvelope<VehicleRow>[] events) => _vehicles.ProjectAsync(events, CancellationToken.None);

    private Task<ProjectionResult> Apply(params DebeziumEnvelope<PositionRow>[] events) => _positions.ProjectAsync(events, CancellationToken.None);

    [Fact(DisplayName = "TB-01 vehicle state update is reflected")]
    public async Task State_update_is_reflected()
    {
        await Apply(Insert(1, lsn: 100, "REGISTERED"));
        await Apply(Update(1, lsn: 200, "ACTIVE", atSeconds: 5));

        var view = await valkey.HashAsync(1);
        Assert.Equal("ACTIVE", view[F.State]);
        Assert.Equal(F.FormatTimestamp(T0.AddSeconds(5)), view[F.StateUpdatedAt]);
        Assert.Equal("0", view[F.Deleted]);
    }

    [Fact(DisplayName = "TB-02 new position is reflected")]
    public async Task New_position_is_reflected()
    {
        await Apply(Position(1, positionId: 10, recordedAtSeconds: 1, lat: -23.1, lng: -46.1));
        await Apply(Position(1, positionId: 11, recordedAtSeconds: 2, lat: -23.2, lng: -46.2));

        var view = await valkey.HashAsync(1);
        Assert.Equal("-23.2", view[F.Lat]);
        Assert.Equal("11", view[F.PositionId]);
    }

    [Fact(DisplayName = "TB-03 out-of-order position is ignored when stale")]
    public async Task Stale_position_is_ignored()
    {
        await Apply(Position(1, positionId: 11, recordedAtSeconds: 20, lat: -23.2));
        var result = await Apply(Position(1, positionId: 12, recordedAtSeconds: 10, lat: -99)); // newer row, older business time

        Assert.Equal(0, result.Applied);
        Assert.Equal("-23.2", (await valkey.HashAsync(1))[F.Lat]);
    }

    [Fact(DisplayName = "TB-04 out-of-order state is ignored when stale (older LSN)")]
    public async Task Stale_state_is_ignored()
    {
        await Apply(Update(1, lsn: 300, "MAINTENANCE"));
        var result = await Apply(Update(1, lsn: 200, "ACTIVE")); // redelivered older change

        Assert.Equal(0, result.Applied);
        Assert.Equal("MAINTENANCE", (await valkey.HashAsync(1))[F.State]);
    }

    [Fact(DisplayName = "TB-05 duplicate CDC event is a no-op")]
    public async Task Duplicate_event_is_noop()
    {
        await Apply(Update(1, lsn: 300, "IDLE"));
        await Apply(Position(1, positionId: 5, recordedAtSeconds: 5));
        var before = await valkey.DigestAsync();

        var state = await Apply(Update(1, lsn: 300, "IDLE"));
        var position = await Apply(Position(1, positionId: 5, recordedAtSeconds: 5));

        Assert.Equal(0, state.Applied + position.Applied);
        Assert.Equal(before, await valkey.DigestAsync());
    }

    [Fact(DisplayName = "TB-06 delete flags the vehicle and late events do not resurrect it")]
    public async Task Delete_flags_vehicle()
    {
        await Apply(Insert(1, lsn: 100, "ACTIVE"));
        await Apply(Position(1, positionId: 1, recordedAtSeconds: 1));
        await Apply(Delete(1, lsn: 500));
        await Apply(Update(1, lsn: 400, "IDLE")); // late update from before the delete
        await Apply(Position(1, positionId: 2, recordedAtSeconds: 2)); // late position

        var view = await valkey.HashAsync(1);
        Assert.Equal("1", view[F.Deleted]);
        Assert.False(view.ContainsKey(F.State));
        Assert.Equal(VersionToken.FromLsn(500), view[F.StateVersion]);
    }

    [Fact(DisplayName = "TB-06b re-insert after delete (higher LSN) resurrects the vehicle")]
    public async Task Reinsert_after_delete_resurrects()
    {
        await Apply(Insert(1, lsn: 100, "ACTIVE"));
        await Apply(Delete(1, lsn: 500));
        await Apply(Insert(1, lsn: 600, "REGISTERED"));

        var view = await valkey.HashAsync(1);
        Assert.Equal("0", view[F.Deleted]);
        Assert.Equal("REGISTERED", view[F.State]);
    }

    [Fact(DisplayName = "TB-07 position before the vehicle exists, then the vehicle arrives")]
    public async Task Position_before_vehicle()
    {
        await Apply(Position(9, positionId: 1, recordedAtSeconds: 1));

        var orphan = await valkey.HashAsync(9);
        Assert.False(orphan.ContainsKey(F.State)); // readers see "state unknown yet"
        Assert.Equal("1", orphan[F.PositionId]);

        await Apply(Insert(9, lsn: 100, "REGISTERED"));

        var joined = await valkey.HashAsync(9);
        Assert.Equal("REGISTERED", joined[F.State]);
        Assert.Equal("1", joined[F.PositionId]);
    }

    [Fact(DisplayName = "TB-08 redelivery of an already applied batch after restart changes nothing")]
    public async Task Redelivery_after_restart_is_noop()
    {
        DebeziumEnvelope<VehicleRow>[] batch = [Insert(1, 100, "REGISTERED"), Update(1, 200, "ACTIVE"), Update(1, 300, "IDLE")];
        await Apply(batch);
        var before = await valkey.DigestAsync();

        var redelivered = await Apply(batch);

        Assert.Equal(0, redelivered.Applied);
        Assert.Equal(before, await valkey.DigestAsync());
    }

    [Fact(DisplayName = "TB-10 snapshot to streaming overlap converges")]
    public async Task Snapshot_then_streaming_overlap()
    {
        // Streaming update committed after the snapshot point, delivered before the snapshot row (cross-partition / restart).
        await Apply(Update(1, lsn: 2_000, "ACTIVE"));
        await Apply(Snapshot(1, lsn: 1_000, "REGISTERED"));

        Assert.Equal("ACTIVE", (await valkey.HashAsync(1))[F.State]);
    }

    [Fact(DisplayName = "In-batch coalescing writes once per vehicle and keeps the max")]
    public async Task Batch_is_coalesced()
    {
        var result = await Apply(
            Position(1, positionId: 1, recordedAtSeconds: 1),
            Position(1, positionId: 3, recordedAtSeconds: 9, lat: -1),
            Position(1, positionId: 2, recordedAtSeconds: 5),
            Position(2, positionId: 4, recordedAtSeconds: 1));

        Assert.Equal(4, result.Received);
        Assert.Equal(2, result.Written);
        Assert.Equal("-1", (await valkey.HashAsync(1))[F.Lat]);
    }
}
