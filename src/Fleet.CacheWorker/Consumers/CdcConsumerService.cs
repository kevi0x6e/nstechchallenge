using System.Diagnostics;
using Confluent.Kafka;
using Fleet.CacheWorker.Cdc;
using Fleet.CacheWorker.Configuration;
using Fleet.CacheWorker.Diagnostics;
using Fleet.CacheWorker.Projection;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Fleet.CacheWorker.Consumers;

/// <summary>
/// One Kafka consumer per CDC topic, on a dedicated thread (librdkafka's Consume is blocking).
/// Loop: fill a micro-batch → project (conditional Valkey writes) → store offsets.
/// </summary>
/// <remarks>
/// Delivery is at-least-once: offsets are stored only after Valkey acknowledged the batch
/// (<c>enable.auto.offset.store=false</c>) and committed in the background by librdkafka.
/// A crash between write and commit re-delivers the batch, which the version guard turns into no-ops.
/// </remarks>
public sealed partial class CdcConsumerService<TRow> : BackgroundService
    where TRow : class
{
    private static readonly TimeSpan PollTimeout = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan SummaryInterval = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(5);

    private readonly string _topic;
    private readonly ICdcProjector<TRow> _projector;
    private readonly IDeserializer<DebeziumEnvelope<TRow>?> _deserializer;
    private readonly KafkaOptions _options;
    private readonly CacheWorkerMetrics _metrics;
    private readonly ILogger _logger;

    public CdcConsumerService(
        string topic,
        ICdcProjector<TRow> projector,
        IDeserializer<DebeziumEnvelope<TRow>?> deserializer,
        IOptions<KafkaOptions> options,
        CacheWorkerMetrics metrics,
        ILogger<CdcConsumerService<TRow>> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        _topic = topic;
        _projector = projector;
        _deserializer = deserializer;
        _options = options.Value;
        _metrics = metrics;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Factory.StartNew(() => Run(stoppingToken), stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    private void Run(CancellationToken ct)
    {
        using var consumer = BuildConsumer();
        consumer.Subscribe(_topic);
        LogStarted(_projector.Stream, _topic);

        var envelopes = new List<DebeziumEnvelope<TRow>>(_options.MaxBatchSize);
        var offsets = new Dictionary<TopicPartition, TopicPartitionOffset>();
        var totals = new ProjectionTotals();
        var summaryClock = Stopwatch.StartNew();

        try
        {
            while (!ct.IsCancellationRequested)
            {
                FillBatch(consumer, envelopes, offsets, ct);

                if (offsets.Count > 0)
                {
                    var result = ProjectWithRetry(envelopes, ct);
                    StoreOffsets(consumer, offsets.Values);
                    totals.Add(result);
                    _metrics.RecordBatch(_projector.Stream, result.Received, result.Applied, result.Stale);
                }

                envelopes.Clear();
                offsets.Clear();

                if (summaryClock.Elapsed >= SummaryInterval)
                {
                    LogSummary(_projector.Stream, totals.Received, totals.Applied, totals.Stale);
                    summaryClock.Restart();
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown: the in-flight batch (if any) was not acknowledged and will be re-delivered.
        }
        finally
        {
            consumer.Close(); // commits stored offsets and leaves the group cleanly
            LogStopped(_projector.Stream);
        }
    }

    /// <summary>Waits up to <see cref="PollTimeout"/> for the first message, then drains what arrives within the batch window.</summary>
    private void FillBatch(
        IConsumer<Ignore, DebeziumEnvelope<TRow>?> consumer,
        List<DebeziumEnvelope<TRow>> envelopes,
        Dictionary<TopicPartition, TopicPartitionOffset> offsets,
        CancellationToken ct)
    {
        if (!ConsumeOne(consumer, PollTimeout, envelopes, offsets, ct))
        {
            return; // idle: let the outer loop observe cancellation and log
        }

        var maxWait = TimeSpan.FromMilliseconds(_options.MaxBatchWaitMs);
        var window = Stopwatch.StartNew();
        while (envelopes.Count < _options.MaxBatchSize && window.Elapsed < maxWait)
        {
            if (!ConsumeOne(consumer, maxWait - window.Elapsed, envelopes, offsets, ct))
            {
                break;
            }
        }
    }

    /// <returns>false when the poll timed out without a message.</returns>
    private bool ConsumeOne(
        IConsumer<Ignore, DebeziumEnvelope<TRow>?> consumer,
        TimeSpan timeout,
        List<DebeziumEnvelope<TRow>> envelopes,
        Dictionary<TopicPartition, TopicPartitionOffset> offsets,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        ConsumeResult<Ignore, DebeziumEnvelope<TRow>?>? record;
        try
        {
            record = consumer.Consume(timeout);
        }
        catch (ConsumeException ex) when (ex.Error.Code is ErrorCode.Local_ValueDeserialization or ErrorCode.Local_KeyDeserialization)
        {
            // Poison message: skip it but keep offset order. Production would route it to a DLQ.
            LogPoisonMessage(ex, _projector.Stream, ex.ConsumerRecord.TopicPartitionOffset);
            _metrics.RecordSkipped(_projector.Stream, "undecodable");
            Track(offsets, ex.ConsumerRecord.TopicPartitionOffset);
            return true;
        }

        if (record is null)
        {
            return false;
        }

        if (record.Message.Value is { } envelope)
        {
            envelopes.Add(envelope);
        }
        else
        {
            _metrics.RecordSkipped(_projector.Stream, "tombstone"); // Debezium's post-delete compaction marker
        }

        Track(offsets, record.TopicPartitionOffset);
        return true;
    }

    private static void Track(Dictionary<TopicPartition, TopicPartitionOffset> offsets, TopicPartitionOffset position) =>
        offsets[position.TopicPartition] = new TopicPartitionOffset(position.TopicPartition, position.Offset + 1);

    /// <summary>
    /// Valkey unavailability must not skip events: retry the same batch (it is idempotent)
    /// with capped exponential backoff, holding the partition until it succeeds.
    /// </summary>
    private ProjectionResult ProjectWithRetry(List<DebeziumEnvelope<TRow>> envelopes, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                // Sync-over-async is deliberate: this is a dedicated thread without a SynchronizationContext.
                return _projector.ProjectAsync(envelopes, ct).GetAwaiter().GetResult();
            }
            catch (Exception ex) when (ex is RedisException or TimeoutException)
            {
                var delay = TimeSpan.FromMilliseconds(Math.Min(MaxRetryDelay.TotalMilliseconds, 100 * Math.Pow(2, attempt)));
                LogWriteRetry(ex, _projector.Stream, attempt, delay);
                if (ct.WaitHandle.WaitOne(delay))
                {
                    ct.ThrowIfCancellationRequested();
                }
            }
        }
    }

    private void StoreOffsets(IConsumer<Ignore, DebeziumEnvelope<TRow>?> consumer, IEnumerable<TopicPartitionOffset> offsets)
    {
        foreach (var offset in offsets)
        {
            try
            {
                consumer.StoreOffset(offset);
            }
            catch (KafkaException ex) when (ex.Error.Code == ErrorCode.Local_State)
            {
                // Partition was revoked meanwhile: its new owner re-processes from the last commit (idempotent).
                LogRevokedOffset(_projector.Stream, offset);
            }
        }
    }

    private IConsumer<Ignore, DebeziumEnvelope<TRow>?> BuildConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.GroupId,
            ClientId = $"{Environment.MachineName}-{_projector.Stream}",
            AutoOffsetReset = AutoOffsetReset.Earliest,  // a new group rebuilds the view from the beginning
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 1_000,
            EnableAutoOffsetStore = false,               // we store only after Valkey acked
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            EnablePartitionEof = false,
        };

        return new ConsumerBuilder<Ignore, DebeziumEnvelope<TRow>?>(config)
            .SetValueDeserializer(_deserializer)
            .SetPartitionsAssignedHandler((_, partitions) => LogAssigned(_projector.Stream, string.Join(',', partitions)))
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                LogRevoked(_projector.Stream, string.Join(',', partitions));
                try
                {
                    c.Commit(); // flush stored offsets before ownership moves
                }
                catch (KafkaException ex)
                {
                    LogCommitFailed(ex, _projector.Stream);
                }
            })
            .SetErrorHandler((_, error) => LogKafkaError(_projector.Stream, error.Reason))
            .Build();
    }

    private sealed class ProjectionTotals
    {
        public long Received { get; private set; }
        public long Applied { get; private set; }
        public long Stale { get; private set; }

        public void Add(ProjectionResult result)
        {
            Received += result.Received;
            Applied += result.Applied;
            Stale += result.Stale;
        }
    }

    [LoggerMessage(LogLevel.Information, "[{Stream}] consuming {Topic}")]
    private partial void LogStarted(string stream, string topic);

    [LoggerMessage(LogLevel.Information, "[{Stream}] stopped")]
    private partial void LogStopped(string stream);

    [LoggerMessage(LogLevel.Information, "[{Stream}] totals: received={Received} applied={Applied} stale={Stale}")]
    private partial void LogSummary(string stream, long received, long applied, long stale);

    [LoggerMessage(LogLevel.Information, "[{Stream}] assigned: {Partitions}")]
    private partial void LogAssigned(string stream, string partitions);

    [LoggerMessage(LogLevel.Information, "[{Stream}] revoked: {Partitions}")]
    private partial void LogRevoked(string stream, string partitions);

    [LoggerMessage(LogLevel.Error, "[{Stream}] undecodable message skipped at {Position}")]
    private partial void LogPoisonMessage(Exception ex, string stream, TopicPartitionOffset position);

    [LoggerMessage(LogLevel.Warning, "[{Stream}] Valkey write failed (attempt {Attempt}), retrying in {Delay}")]
    private partial void LogWriteRetry(Exception ex, string stream, int attempt, TimeSpan delay);

    [LoggerMessage(LogLevel.Warning, "[{Stream}] offset {Offset} not stored: partition no longer assigned")]
    private partial void LogRevokedOffset(string stream, TopicPartitionOffset offset);

    [LoggerMessage(LogLevel.Warning, "[{Stream}] commit on revoke failed")]
    private partial void LogCommitFailed(Exception ex, string stream);

    [LoggerMessage(LogLevel.Warning, "[{Stream}] kafka: {Reason}")]
    private partial void LogKafkaError(string stream, string reason);
}
