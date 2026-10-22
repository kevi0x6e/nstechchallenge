using System.Globalization;
using Fleet.MockWriters;
using Fleet.Scenario;
using Npgsql;

// Mock writers: drive the source PostgreSQL only. Kafka and Valkey are reached exclusively through CDC.
//
//   snapshot                 write the pre-connector data set (captured by the Debezium snapshot)
//   stream                   run the vehicle and position writers concurrently (streaming phase)
//   vehicles | positions     run a single writer's streaming sequence
//
// Options: --seed N  --positions-rate R  --vehicles-rate R  (rows/s, 0 = as fast as possible)
// Connection: PG_CONNECTION env var.

var command = args.FirstOrDefault() ?? "stream";
var options = CliArgs.Parse(args.Skip(1));
var plan = ScenarioPlan.Generate(new ScenarioOptions { Seed = options.Get("seed", 42) });
var positionsRate = options.Get("positions-rate", 1_000.0);
var vehiclesRate = options.Get("vehicles-rate", 40.0);

var connectionString = Environment.GetEnvironmentVariable("PG_CONNECTION")
    ?? "Host=localhost;Username=fleet;Password=fleet;Database=fleet;GSS Encryption Mode=Disable";
await using var db = NpgsqlDataSource.Create(connectionString);

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var vehicles = new VehicleWriter(db);
var positions = new PositionWriter(db);

switch (command)
{
    case "snapshot":
        await vehicles.WriteAsync(plan.SnapshotVehicles, ratePerSecond: 0, cts.Token);
        await positions.WriteAsync(plan.SnapshotPositions, ratePerSecond: 0, cts.Token);
        break;
    case "stream":
        // Independent writers, like independent producers in production: the position-before-vehicle race happens naturally.
        await Task.WhenAll(
            vehicles.WriteAsync(plan.StreamVehicles, vehiclesRate, cts.Token),
            positions.WriteAsync(plan.StreamPositions, positionsRate, cts.Token));
        break;
    case "vehicles":
        await vehicles.WriteAsync(plan.StreamVehicles, vehiclesRate, cts.Token);
        break;
    case "positions":
        await positions.WriteAsync(plan.StreamPositions, positionsRate, cts.Token);
        break;
    default:
        await Console.Error.WriteLineAsync($"unknown command '{command}' (snapshot|stream|vehicles|positions)");
        return 2;
}

return 0;
