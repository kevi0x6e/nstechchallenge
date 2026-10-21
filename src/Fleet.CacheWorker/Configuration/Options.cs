using System.ComponentModel.DataAnnotations;

namespace Fleet.CacheWorker.Configuration;

public sealed class KafkaOptions
{
    public const string Section = "Kafka";

    [Required] public string BootstrapServers { get; set; } = "localhost:9092";
    [Required] public string GroupId { get; set; } = "fleet-cache-worker";
    [Required] public string VehiclesTopic { get; set; } = "fleet.public.vehicles";
    [Required] public string PositionsTopic { get; set; } = "fleet.public.positions";

    /// <summary>Upper bound of envelopes projected per Valkey pipeline.</summary>
    [Range(1, 50_000)] public int MaxBatchSize { get; set; } = 2_000;

    /// <summary>Max time spent filling a batch once the first message arrived (latency vs. throughput knob).</summary>
    [Range(1, 5_000)] public int MaxBatchWaitMs { get; set; } = 20;
}

public sealed class ValkeyOptions
{
    public const string Section = "Valkey";

    [Required] public string ConnectionString { get; set; } = "localhost:6379";
}
