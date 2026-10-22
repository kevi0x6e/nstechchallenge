using Fleet.CacheWorker.Cdc;
using Fleet.CacheWorker.Projection;
using Fleet.CacheWorker.Valkey;
using static Fleet.CacheWorker.Tests.Cdc;

namespace Fleet.CacheWorker.Tests;

/// <summary>
/// TB-09 as a property: the view is a pure function of the *set* of CDC events.
/// Any delivery order, interleaving between the two streams, batch split and
/// duplication must produce a byte-identical cache.
/// </summary>
[Collection(ValkeyCollection.Name)]
public sealed class ReplayConvergenceTests(ValkeyFixture valkey)
{
    private static readonly string[] States = ["REGISTERED", "ACTIVE", "IDLE", "MAINTENANCE", "DECOMMISSIONED"];

    [Fact(DisplayName = "TB-09 any order, split and duplication of the stream rebuilds the identical cache")]
    public async Task Shuffled_duplicated_replays_converge()
    {
        var (vehicleEvents, positionEvents) = GenerateHistory(seed: 7, vehicles: 25, changesPerVehicle: 12, positionsPerVehicle: 40);

        // Reference: in-log order, one event at a time (what a fresh consumer from offset 0 does).
        await valkey.FlushAsync();
        await ReplayAsync(vehicleEvents, positionEvents, new Random(0), shuffle: false);
        var reference = await valkey.DigestAsync();

        for (var run = 1; run <= 10; run++)
        {
            await valkey.FlushAsync();
            await ReplayAsync(vehicleEvents, positionEvents, new Random(run), shuffle: true);
            Assert.Equal(reference, await valkey.DigestAsync());
        }
    }

    private async Task ReplayAsync(
        List<DebeziumEnvelope<VehicleRow>> vehicleEvents,
        List<DebeziumEnvelope<PositionRow>> positionEvents,
        Random random,
        bool shuffle)
    {
        var store = new ValkeyVehicleViewStore(valkey.Connection);
        var vehicles = new VehicleStateProjector(store);
        var positions = new PositionProjector(store);

        var v = shuffle ? WithDuplicates(vehicleEvents, random) : vehicleEvents;
        var p = shuffle ? WithDuplicates(positionEvents, random) : positionEvents;

        // Interleave the two streams with random batch sizes, like two consumers with independent lag.
        int vi = 0, pi = 0;
        while (vi < v.Count || pi < p.Count)
        {
            var size = shuffle ? random.Next(1, 64) : 1;
            if (pi >= p.Count || (vi < v.Count && random.Next(2) == 0))
            {
                var take = v.Skip(vi).Take(size).ToList();
                vi += take.Count;
                await vehicles.ProjectAsync(take, CancellationToken.None);
            }
            else
            {
                var take = p.Skip(pi).Take(size).ToList();
                pi += take.Count;
                await positions.ProjectAsync(take, CancellationToken.None);
            }
        }
    }

    private static List<T> WithDuplicates<T>(List<T> events, Random random)
    {
        var withDupes = events.Concat(events.Where(_ => random.NextDouble() < 0.2)).ToArray();
        random.Shuffle(withDupes);
        return [.. withDupes];
    }

    private static (List<DebeziumEnvelope<VehicleRow>>, List<DebeziumEnvelope<PositionRow>>) GenerateHistory(
        int seed, int vehicles, int changesPerVehicle, int positionsPerVehicle)
    {
        var random = new Random(seed);
        var lsn = 1_000L;
        var positionId = 0L;
        var vehicleEvents = new List<DebeziumEnvelope<VehicleRow>>();
        var positionEvents = new List<DebeziumEnvelope<PositionRow>>();

        for (var id = 1; id <= vehicles; id++)
        {
            vehicleEvents.Add(id % 3 == 0 ? Snapshot(id, lsn, "REGISTERED") : Insert(id, lsn += 10, "REGISTERED"));
            for (var c = 0; c < changesPerVehicle; c++)
            {
                vehicleEvents.Add(Update(id, lsn += 10, States[random.Next(States.Length)], atSeconds: random.Next(0, 3_600)));
            }

            if (id % 5 == 0)
            {
                vehicleEvents.Add(Delete(id, lsn += 10));
                if (id % 10 == 0)
                {
                    vehicleEvents.Add(Insert(id, lsn += 10, "REGISTERED")); // resurrected
                }
            }

            for (var k = 0; k < positionsPerVehicle; k++)
            {
                // Business time deliberately non-monotonic (late telemetry) and with ties.
                positionEvents.Add(Position(id, ++positionId, random.Next(0, 600), random.NextDouble() * 10, random.NextDouble() * 10));
            }
        }

        // Positions for a vehicle that never gets a vehicle row (orphan stream).
        positionEvents.Add(Position(999, ++positionId, 5));
        return (vehicleEvents, positionEvents);
    }
}
