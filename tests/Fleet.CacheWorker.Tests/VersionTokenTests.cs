using Fleet.CacheWorker.Projection;
using Fleet.Contracts;

namespace Fleet.CacheWorker.Tests;

public sealed class VersionTokenTests
{
    [Theory]
    [InlineData(9, 10)]
    [InlineData(99_999, 100_000)]
    [InlineData(9_007_199_254_740_992, 9_007_199_254_740_993)] // 2^53 and 2^53+1: indistinguishable as Lua doubles
    public void Lsn_tokens_order_lexicographically_like_numbers(long older, long newer)
    {
        Assert.True(string.CompareOrdinal(VersionToken.FromLsn(older), VersionToken.FromLsn(newer)) < 0);
    }

    [Fact]
    public void Position_tokens_order_by_recorded_at_then_position_id()
    {
        var t = new DateTimeOffset(2026, 10, 20, 10, 0, 0, TimeSpan.Zero);

        Assert.True(string.CompareOrdinal(VersionToken.FromPosition(t, 999), VersionToken.FromPosition(t.AddTicks(10), 1)) < 0);
        Assert.True(string.CompareOrdinal(VersionToken.FromPosition(t, 1), VersionToken.FromPosition(t, 2)) < 0);
    }

    [Fact]
    public void Latest_wins_keeps_highest_version_per_vehicle()
    {
        var events = new[]
        {
            new VehicleStateChange(1, VersionToken.FromLsn(30), false, "IDLE", null),
            new VehicleStateChange(1, VersionToken.FromLsn(50), false, "ACTIVE", null),
            new VehicleStateChange(1, VersionToken.FromLsn(40), false, "MAINTENANCE", null),
            new VehicleStateChange(2, VersionToken.FromLsn(10), true, null, null),
        };

        var latest = LatestWins.PerVehicle(events).ToDictionary(e => e.VehicleId);

        Assert.Equal(2, latest.Count);
        Assert.Equal("ACTIVE", latest[1].State);
        Assert.True(latest[2].Deleted);
    }
}
