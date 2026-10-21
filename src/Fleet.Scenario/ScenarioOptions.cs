namespace Fleet.Scenario;

/// <summary>Knobs of the deterministic scenario. Same options ⇒ same sequence of writes.</summary>
public sealed record ScenarioOptions
{
    public int Seed { get; init; } = 42;

    /// <summary>Vehicles (ids 1..N) written before the connector exists: captured by the Debezium snapshot.</summary>
    public int SnapshotVehicles { get; init; } = 30;
    public int SnapshotPositionsPerVehicle { get; init; } = 5;

    /// <summary>Vehicles inserted during streaming.</summary>
    public int StreamVehicles { get; init; } = 10;

    /// <summary>Vehicles whose positions are written before the vehicle row (cross-stream race).</summary>
    public int RaceVehicles { get; init; } = 5;

    public int StateChangesPerVehicle { get; init; } = 8;
    public int StreamPositionsPerVehicle { get; init; } = 80;

    /// <summary>Every k-th vehicle id is deleted; every (k*2)-th is re-inserted afterwards.</summary>
    public int DeleteEvery { get; init; } = 7;

    public double LateProbability { get; init; } = 0.10;
    public double DuplicateProbability { get; init; } = 0.05;

    public DateTimeOffset BaseTime { get; init; } = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
}
