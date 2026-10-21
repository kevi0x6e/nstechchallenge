namespace Fleet.Scenario;

/// <summary>
/// A fixed, seed-determined sequence of source-DB writes covering every hard case:
/// snapshot vs. streaming, out-of-order business times, duplicate inserts, deletes,
/// re-inserts after delete and positions for vehicles that do not exist yet.
/// Shared by the mock writers (to write it) and the e2e verifier (to know which vehicles were deleted).
/// </summary>
public sealed record ScenarioPlan(
    IReadOnlyList<VehicleOp> SnapshotVehicles,
    IReadOnlyList<InsertPosition> SnapshotPositions,
    IReadOnlyList<VehicleOp> StreamVehicles,
    IReadOnlyList<InsertPosition> StreamPositions)
{
    /// <summary>Every vehicle id that had a row at some point (deleted ones included).</summary>
    public IReadOnlySet<long> EverCreatedVehicleIds =>
        SnapshotVehicles.Concat(StreamVehicles).OfType<InsertVehicle>().Select(v => v.VehicleId).ToHashSet();

    public static ScenarioPlan Generate(ScenarioOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new Generator(options).Build();
    }

    private sealed class Generator(ScenarioOptions o)
    {
        private readonly Random _random = new(o.Seed);
        private readonly Dictionary<long, DateTimeOffset> _stateClock = [];
        private readonly Dictionary<long, DateTimeOffset> _positionClock = [];
        private readonly Dictionary<long, (double Lat, double Lng)> _location = [];

        public ScenarioPlan Build()
        {
            var snapshotIds = Range(1, o.SnapshotVehicles);
            var streamIds = Range(o.SnapshotVehicles + 1, o.StreamVehicles);
            var raceIds = Range(o.SnapshotVehicles + o.StreamVehicles + 1, o.RaceVehicles);
            var allIds = snapshotIds.Concat(streamIds).Concat(raceIds).ToArray();

            var snapshotVehicles = snapshotIds.Select(Insert).ToList<VehicleOp>();
            var snapshotPositions = snapshotIds
                .SelectMany(id => Enumerable.Range(0, o.SnapshotPositionsPerVehicle).Select(_ => NextPosition(id, allowDisorder: false)))
                .ToList();

            // Vehicle stream: per-vehicle histories interleaved randomly; race vehicles are inserted last.
            var histories = snapshotIds.Select(id => History(id, insert: false))
                .Concat(streamIds.Select(id => History(id, insert: true)))
                .ToList();
            var streamVehicles = Interleave(histories);
            streamVehicles.AddRange(raceIds.Select(Insert));

            // Position stream: race vehicles report first (their vehicle row does not exist yet).
            var streamPositions = raceIds.SelectMany(id => Enumerable.Range(0, 3).Select(_ => NextPosition(id, allowDisorder: false))).ToList();
            var others = allIds
                .SelectMany(id => Enumerable.Range(0, o.StreamPositionsPerVehicle).Select(_ => id))
                .ToArray();
            _random.Shuffle(others);
            foreach (var id in others)
            {
                var position = NextPosition(id, allowDisorder: true);
                streamPositions.Add(position);
                if (_random.NextDouble() < o.DuplicateProbability)
                {
                    streamPositions.Add(position with { IsDuplicate = true });
                }
            }

            return new ScenarioPlan(snapshotVehicles, snapshotPositions, streamVehicles, streamPositions);
        }

        private List<VehicleOp> History(long id, bool insert)
        {
            var ops = new List<VehicleOp>();
            var state = VehicleStates.Registered;
            if (insert)
            {
                ops.Add(Insert(id));
            }

            var deleteAt = id % o.DeleteEvery == 0 ? _random.Next(1, o.StateChangesPerVehicle) : -1;
            for (var step = 0; step < o.StateChangesPerVehicle; step++)
            {
                var next = VehicleStates.Transitions[state];
                if (step == deleteAt || (next.Length == 0 && deleteAt > step))
                {
                    deleteAt = -1;
                    ops.Add(new DeleteVehicle(id));
                    if (id % (o.DeleteEvery * 2) != 0)
                    {
                        return ops; // stays deleted
                    }

                    ops.Add(Insert(id)); // re-inserted with the same id
                    state = VehicleStates.Registered;
                    continue;
                }

                if (next.Length == 0)
                {
                    break; // terminal state (DECOMMISSIONED)
                }

                state = next[_random.Next(next.Length)];
                var late = _random.NextDouble() < o.LateProbability;
                ops.Add(new UpdateVehicleState(id, state, NextStateTime(id, late), late));
            }

            return ops;
        }

        private InsertVehicle Insert(long id)
        {
            _stateClock[id] = _stateClock.TryGetValue(id, out var t) ? t.AddMinutes(1) : o.BaseTime.AddSeconds(id);
            return new InsertVehicle(id, $"FLT{id:D4}", VehicleStates.Registered, _stateClock[id]);
        }

        private DateTimeOffset NextStateTime(long id, bool late)
        {
            if (late)
            {
                return _stateClock[id].AddMinutes(-_random.Next(1, 30)); // does not move the clock
            }

            _stateClock[id] = _stateClock[id].AddMinutes(_random.Next(1, 10));
            return _stateClock[id];
        }

        private InsertPosition NextPosition(long id, bool allowDisorder)
        {
            var (lat, lng) = _location.TryGetValue(id, out var loc)
                ? loc
                : (-23.55 + (id * 0.01), -46.63 - (id * 0.01));
            lat += (_random.NextDouble() - 0.5) * 0.001;
            lng += (_random.NextDouble() - 0.5) * 0.001;
            _location[id] = (Math.Round(lat, 6), Math.Round(lng, 6));

            var clock = _positionClock.TryGetValue(id, out var c) ? c : o.BaseTime;
            if (allowDisorder && _random.NextDouble() < o.LateProbability)
            {
                return new InsertPosition(id, _location[id].Lat, _location[id].Lng, clock.AddSeconds(-_random.Next(5, 600)), IsLate: true, IsDuplicate: false);
            }

            _positionClock[id] = clock.AddSeconds(_random.Next(1, 5)).AddMilliseconds(_random.Next(0, 999));
            return new InsertPosition(id, _location[id].Lat, _location[id].Lng, _positionClock[id], IsLate: false, IsDuplicate: false);
        }

        private List<VehicleOp> Interleave(List<List<VehicleOp>> histories)
        {
            var cursors = histories.Where(h => h.Count > 0).Select(h => new Queue<VehicleOp>(h)).ToList();
            var result = new List<VehicleOp>();
            while (cursors.Count > 0)
            {
                var i = _random.Next(cursors.Count);
                result.Add(cursors[i].Dequeue());
                if (cursors[i].Count == 0)
                {
                    cursors.RemoveAt(i);
                }
            }

            return result;
        }

        private static long[] Range(int start, int count) => Enumerable.Range(start, count).Select(i => (long)i).ToArray();
    }
}
