using System.Text.Json.Serialization;

namespace Fleet.CacheWorker.Cdc;

/// <summary>Source-generated serializer metadata: no reflection on the hot path.</summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.SnakeCaseLower)]
[JsonSerializable(typeof(DebeziumEnvelope<VehicleRow>))]
[JsonSerializable(typeof(DebeziumEnvelope<PositionRow>))]
public sealed partial class CdcJsonContext : JsonSerializerContext;
