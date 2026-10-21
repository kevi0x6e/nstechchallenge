using Fleet.CacheWorker.Cdc;
using Fleet.CacheWorker.Configuration;
using Fleet.CacheWorker.Consumers;
using Fleet.CacheWorker.Diagnostics;
using Fleet.CacheWorker.Projection;
using Fleet.CacheWorker.Valkey;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder.Services.AddOptions<ValkeyOptions>()
    .Bind(builder.Configuration.GetSection(ValkeyOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var options = ConfigurationOptions.Parse(sp.GetRequiredService<IOptions<ValkeyOptions>>().Value.ConnectionString);
    options.AbortOnConnectFail = false; // keep reconnecting; writes retry until Valkey is back
    return ConnectionMultiplexer.Connect(options);
});
builder.Services.AddSingleton<IVehicleViewStore, ValkeyVehicleViewStore>();
builder.Services.AddSingleton<CacheWorkerMetrics>();

// Two independent consumers: the high-frequency position stream never delays state changes.
builder.Services.AddHostedService(sp => new CdcConsumerService<VehicleRow>(
    sp.GetRequiredService<IOptions<KafkaOptions>>().Value.VehiclesTopic,
    new VehicleStateProjector(sp.GetRequiredService<IVehicleViewStore>()),
    new JsonValueDeserializer<DebeziumEnvelope<VehicleRow>>(CdcJsonContext.Default.DebeziumEnvelopeVehicleRow),
    sp.GetRequiredService<IOptions<KafkaOptions>>(),
    sp.GetRequiredService<CacheWorkerMetrics>(),
    sp.GetRequiredService<ILogger<CdcConsumerService<VehicleRow>>>()));
builder.Services.AddHostedService(sp => new CdcConsumerService<PositionRow>(
    sp.GetRequiredService<IOptions<KafkaOptions>>().Value.PositionsTopic,
    new PositionProjector(sp.GetRequiredService<IVehicleViewStore>()),
    new JsonValueDeserializer<DebeziumEnvelope<PositionRow>>(CdcJsonContext.Default.DebeziumEnvelopePositionRow),
    sp.GetRequiredService<IOptions<KafkaOptions>>(),
    sp.GetRequiredService<CacheWorkerMetrics>(),
    sp.GetRequiredService<ILogger<CdcConsumerService<PositionRow>>>()));

await builder.Build().RunAsync();
