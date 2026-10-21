using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using Fleet.Contracts;
using Fleet.E2E;
using Fleet.Scenario;
using Npgsql;
using StackExchange.Redis;

// E2E verifier and cache reader.
//
//   assert  [--seed N] [--timeout S]   wait until the worker caught up, then compare the cache with the DB oracle
//   digest  [--expect HEX] [--timeout S]  wait for quiescence (lag 0, no new CDC), print the cache SHA-256
//   read    --id N                     print one vehicle view (what a tracking UI would read)

var command = args.FirstOrDefault() ?? "assert";
var options = CliArgs.Parse(args.Skip(1));
var timeout = TimeSpan.FromSeconds(options.Get("timeout", 240));

string Env(string name, string fallback) => Environment.GetEnvironmentVariable(name) ?? fallback;

using var valkey = await ConnectionMultiplexer.ConnectAsync(Env("VALKEY", "localhost:6379"));
var cache = new Cache(valkey);

if (command == "read")
{
    var view = await valkey.GetDatabase().HashGetAllAsync(VehicleViewSchema.Key(options.Get("id", 1)));
    var fields = view.ToDictionary(e => e.Name.ToString(), e => e.Value.ToString());
    var status = fields.Count == 0 ? "not-found"
        : fields.GetValueOrDefault(VehicleViewSchema.Deleted) == "1" ? "deleted (404 for readers)"
        : fields.ContainsKey(VehicleViewSchema.State) ? "ok" : "position-only (state unknown yet)";
#pragma warning disable CA1869 // one-shot CLI call
    Console.WriteLine(JsonSerializer.Serialize(new { status, fields }, new JsonSerializerOptions { WriteIndented = true }));
#pragma warning restore CA1869
    return 0;
}

using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Env("KAFKA", "localhost:9092") }).Build();
var lag = new Lag(admin, Env("GROUP_ID", "fleet-cache-worker"), ["fleet.public.vehicles", "fleet.public.positions"]);
var clock = Stopwatch.StartNew();

switch (command)
{
    case "assert":
    {
        var plan = ScenarioPlan.Generate(new ScenarioOptions { Seed = options.Get("seed", 42) });
        await using var db = NpgsqlDataSource.Create(Env("PG_CONNECTION", "Host=localhost;Username=fleet;Password=fleet;Database=fleet;GSS Encryption Mode=Disable"));
        var diffs = new List<string>();
        while (clock.Elapsed < timeout)
        {
            var measured = await lag.MeasureAsync();
            var expected = await Oracle.ExpectedViewAsync(db, plan, CancellationToken.None);
            var actual = await cache.ReadAllAsync();
            diffs = Oracle.Diff(expected, actual);
            Console.WriteLine($"  t={clock.Elapsed.TotalSeconds:F0}s lag={measured?.Lag.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "?"} vehicles={expected.Count} diffs={diffs.Count}");
            if (diffs.Count == 0 && measured is { Lag: 0 })
            {
                var deleted = actual.Values.Count(h => h.GetValueOrDefault(VehicleViewSchema.Deleted) == "1");
                Console.WriteLine($"ASSERT OK: {actual.Count} vehicle views match the database ({deleted} tombstones)");
                return 0;
            }

            await Task.Delay(TimeSpan.FromSeconds(2));
        }

        Console.WriteLine($"ASSERT FAILED after {timeout.TotalSeconds}s, {diffs.Count} differences:");
        diffs.Take(30).ToList().ForEach(d => Console.WriteLine($"  {d}"));
        return 1;
    }

    case "digest":
    {
        // Quiescent = lag 0 and no new CDC records over three consecutive polls.
        var stable = 0;
        long? lastEnd = null;
        while (clock.Elapsed < timeout && stable < 3)
        {
            var measured = await lag.MeasureAsync();
            stable = measured is { Lag: 0 } m && m.EndOffsetsSum == lastEnd ? stable + 1 : 0;
            lastEnd = measured?.EndOffsetsSum;
            Console.WriteLine($"  t={clock.Elapsed.TotalSeconds:F0}s lag={measured?.Lag.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "?"}");
            await Task.Delay(TimeSpan.FromSeconds(2));
        }

        if (stable < 3)
        {
            Console.WriteLine("DIGEST FAILED: worker did not catch up in time");
            return 1;
        }

        var digest = await cache.DigestAsync();
        var keys = (await cache.ReadAllAsync()).Count;
        Console.WriteLine($"DIGEST {digest} ({keys} keys)");
        var expect = options.Get("expect");
        if (!string.IsNullOrEmpty(expect) && expect != digest)
        {
            Console.WriteLine($"DIGEST MISMATCH: expected {expect}");
            return 1;
        }

        return 0;
    }

    default:
        Console.Error.WriteLine($"unknown command '{command}' (assert|digest|read)");
        return 2;
}
