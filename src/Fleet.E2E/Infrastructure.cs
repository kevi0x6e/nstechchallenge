using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Fleet.Contracts;
using StackExchange.Redis;

namespace Fleet.E2E;

internal sealed class Cache(IConnectionMultiplexer valkey)
{
    public async Task<Dictionary<string, Dictionary<string, string>>> ReadAllAsync()
    {
        var db = valkey.GetDatabase();
        var keys = new List<RedisKey>();
        await foreach (var key in valkey.GetServers()[0].KeysAsync(pattern: VehicleViewSchema.KeyPattern, pageSize: 1_000))
        {
            keys.Add(key);
        }

        var hashes = await Task.WhenAll(keys.Select(k => db.HashGetAllAsync(k)));
        return keys.Zip(hashes).ToDictionary(
            x => x.First.ToString(),
            x => x.Second.ToDictionary(e => e.Name.ToString(), e => e.Value.ToString(), StringComparer.Ordinal),
            StringComparer.Ordinal);
    }

    public async Task<string> DigestAsync() =>
        ViewDigest.Compute((await ReadAllAsync()).Select(h =>
            new KeyValuePair<string, IReadOnlyDictionary<string, string>>(h.Key, h.Value)));
}

/// <summary>Consumer-group lag of the cache worker: the "has it caught up?" signal.</summary>
internal sealed class Lag(IAdminClient admin, string groupId, IReadOnlyList<string> topics)
{
    /// <returns>Total lag and the end offsets, or null while topics do not exist yet.</returns>
    public async Task<(long Lag, long EndOffsetsSum)?> MeasureAsync()
    {
        try
        {
            var partitions = topics
                .SelectMany(t => admin.GetMetadata(t, TimeSpan.FromSeconds(10)).Topics
                    .Where(m => m.Error.Code == ErrorCode.NoError)
                    .SelectMany(m => m.Partitions.Select(p => new TopicPartition(t, p.PartitionId))))
                .ToList();
            if (partitions.Count == 0)
            {
                return null;
            }

            var ends = await admin.ListOffsetsAsync(partitions.Select(tp => new TopicPartitionOffsetSpec { TopicPartition = tp, OffsetSpec = OffsetSpec.Latest() }));
            var committed = await admin.ListConsumerGroupOffsetsAsync([new ConsumerGroupTopicPartitions(groupId, partitions)]);
            var committedByPartition = committed.Single().Partitions.ToDictionary(p => p.TopicPartition, p => p.Offset.Value);

            long lag = 0, endSum = 0;
            foreach (var end in ends.ResultInfos.Select(r => r.TopicPartitionOffsetError))
            {
                var done = committedByPartition.GetValueOrDefault(end.TopicPartition, 0);
                lag += end.Offset.Value - Math.Max(done, 0);
                endSum += end.Offset.Value;
            }

            return (lag, endSum);
        }
        catch (KafkaException ex)
        {
            Console.WriteLine($"  lag: not measurable yet ({ex.Error.Reason})");
            return null;
        }
    }
}
