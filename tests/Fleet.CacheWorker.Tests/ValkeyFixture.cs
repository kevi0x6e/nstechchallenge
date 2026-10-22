using Fleet.Contracts;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace Fleet.CacheWorker.Tests;

/// <summary>A real Valkey (same image as the cluster) so the Lua scripts are tested, not mocked.</summary>
public sealed class ValkeyFixture : IAsyncLifetime
{
    private readonly RedisContainer _container = new RedisBuilder("valkey/valkey:8.1-alpine").Build();

    public ConnectionMultiplexer Connection { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        Connection = await ConnectionMultiplexer.ConnectAsync(_container.GetConnectionString() + ",allowAdmin=true");
    }

    public async Task DisposeAsync()
    {
        await Connection.DisposeAsync();
        await _container.DisposeAsync();
    }

    public async Task FlushAsync() => await Connection.GetServers()[0].FlushDatabaseAsync();

    public async Task<Dictionary<string, string>> HashAsync(long vehicleId) =>
        (await Connection.GetDatabase().HashGetAllAsync(VehicleViewSchema.Key(vehicleId)))
            .ToDictionary(e => e.Name.ToString(), e => e.Value.ToString());

    public async Task<string> DigestAsync()
    {
        var db = Connection.GetDatabase();
        var hashes = new List<KeyValuePair<string, IReadOnlyDictionary<string, string>>>();
        await foreach (var key in Connection.GetServers()[0].KeysAsync(pattern: VehicleViewSchema.KeyPattern))
        {
            var entries = await db.HashGetAllAsync(key);
            hashes.Add(new(key.ToString(), entries.ToDictionary(e => e.Name.ToString(), e => e.Value.ToString())));
        }

        return ViewDigest.Compute(hashes);
    }
}

[CollectionDefinition(Name)]
public sealed class ValkeyCollection : ICollectionFixture<ValkeyFixture>
{
    public const string Name = "valkey";
}
