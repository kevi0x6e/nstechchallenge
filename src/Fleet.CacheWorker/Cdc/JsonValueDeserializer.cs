using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Confluent.Kafka;

namespace Fleet.CacheWorker.Cdc;

/// <summary>
/// Deserializes the Kafka value straight from librdkafka's buffer (no intermediate string).
/// A null value is a Kafka tombstone (emitted by Debezium after a delete for log compaction)
/// and is surfaced as <c>null</c> so the consumer can skip it.
/// </summary>
public sealed class JsonValueDeserializer<T>(JsonTypeInfo<T> typeInfo) : IDeserializer<T?>
    where T : class
{
    public T? Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
        isNull ? null : JsonSerializer.Deserialize(data, typeInfo);
}
