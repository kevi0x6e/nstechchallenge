using System.Text;
using Confluent.Kafka;
using Fleet.CacheWorker.Cdc;
using Fleet.CacheWorker.Projection;
using Fleet.Contracts;

namespace Fleet.CacheWorker.Tests;

/// <summary>Parsing of real-shaped Debezium (JSON, schemas.enable=false) payloads.</summary>
public sealed class CdcMapperTests
{
    private static readonly JsonValueDeserializer<DebeziumEnvelope<VehicleRow>> VehicleDeserializer =
        new(CdcJsonContext.Default.DebeziumEnvelopeVehicleRow);

    private static readonly JsonValueDeserializer<DebeziumEnvelope<PositionRow>> PositionDeserializer =
        new(CdcJsonContext.Default.DebeziumEnvelopePositionRow);

    private static T? Parse<T>(IDeserializer<T?> deserializer, string? json) where T : class =>
        deserializer.Deserialize(json is null ? default : Encoding.UTF8.GetBytes(json), json is null, SerializationContext.Empty);

    [Fact]
    public void Snapshot_read_maps_to_state_change_versioned_by_lsn()
    {
        const string json = """
            {"before":null,"after":{"id":7,"plate":"ABC1D23","state":"ACTIVE","state_updated_at":"2026-10-20T10:00:00.123456Z","updated_at":"2026-10-20T10:00:00Z"},
             "source":{"version":"3.3.1.Final","connector":"postgresql","name":"fleet","ts_ms":1760954400000,"snapshot":"true","db":"fleet","sequence":"[null,\"23456\"]","schema":"public","table":"vehicles","txId":770,"lsn":23456,"xmin":null},
             "transaction":null,"op":"r","ts_ms":1760954400100}
            """;

        var change = CdcMapper.ToStateChange(Parse(VehicleDeserializer, json)!);

        Assert.NotNull(change);
        Assert.Equal(7, change.VehicleId);
        Assert.Equal("ACTIVE", change.State);
        Assert.False(change.Deleted);
        Assert.Equal(VersionToken.FromLsn(23456), change.Version);
        Assert.Equal(new DateTimeOffset(2026, 10, 20, 10, 0, 0, TimeSpan.Zero).AddTicks(1_234_560), change.StateUpdatedAt);
    }

    [Fact]
    public void Delete_with_key_only_before_image_maps_to_tombstone()
    {
        // REPLICA IDENTITY DEFAULT: the before image only carries the primary key.
        const string json = """
            {"before":{"id":7,"plate":null,"state":null,"state_updated_at":null,"updated_at":null},"after":null,
             "source":{"ts_ms":1760954500000,"table":"vehicles","lsn":99999},"op":"d","ts_ms":1760954500100}
            """;

        var change = CdcMapper.ToStateChange(Parse(VehicleDeserializer, json)!);

        Assert.NotNull(change);
        Assert.True(change.Deleted);
        Assert.Equal(7, change.VehicleId);
        Assert.Null(change.State);
    }

    [Fact]
    public void Kafka_tombstone_is_surfaced_as_null()
    {
        Assert.Null(Parse(VehicleDeserializer, null));
    }

    [Fact]
    public void Position_insert_maps_with_business_time_version()
    {
        const string json = """
            {"before":null,"after":{"id":42,"vehicle_id":7,"lat":-23.5505,"lng":-46.6333,"recorded_at":"2026-10-20T10:00:05Z"},
             "source":{"ts_ms":1760954405000,"table":"positions","lsn":30000},"op":"c","ts_ms":1760954405100}
            """;

        var position = CdcMapper.ToPosition(Parse(PositionDeserializer, json)!);

        Assert.NotNull(position);
        Assert.Equal(7, position.VehicleId);
        Assert.Equal(42, position.PositionId);
        Assert.Equal(-23.5505, position.Lat);
        Assert.Equal(VersionToken.FromPosition(DateTimeOffset.Parse("2026-10-20T10:00:05Z", System.Globalization.CultureInfo.InvariantCulture), 42), position.Version);
    }

    [Fact]
    public void Non_insert_position_ops_are_ignored()
    {
        const string json = """{"before":{"id":42,"vehicle_id":7,"lat":0,"lng":0,"recorded_at":"2026-10-20T10:00:05Z"},"after":null,"source":{"ts_ms":1,"lsn":1},"op":"d"}""";
        Assert.Null(CdcMapper.ToPosition(Parse(PositionDeserializer, json)!));
    }
}
