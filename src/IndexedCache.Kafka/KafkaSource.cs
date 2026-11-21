using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace IndexedCache;

// A compacted topic, read whole from the beginning on every start: each message an Upsert of its key, a tombstone (null value)
// a Delete. Every instance needs every partition, so they are assigned, not subscribed through a group: no rebalances, no commits.
// A message that fails to deserialize (or has no key) is thrown, never skipped: the cache faults and the service restarts.
// Telemetry: the indexed_cache.kafka.lag gauge, and the client's own logs and errors (broker unreachable, ...) go to the logger.
// shortcut: partitions added to the topic while running are not read until restart.
public sealed class KafkaSource<TKey, TEntity> : ICacheSource<TKey, TEntity> where TEntity : class
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly ConsumerConfig _config;
    private readonly string _topic;
    private readonly IDeserializer<TKey> _keyDeserializer;
    private readonly IDeserializer<TEntity> _valueDeserializer;
    private readonly ILogger _logger;
    // Partition -> messages not read yet, as of the last message or end (EOF) read from it. Empty while not reading.
    private readonly ConcurrentDictionary<int, long> _lag = new();

    public KafkaSource(ConsumerConfig config, string topic, IDeserializer<TKey> keys, IDeserializer<TEntity> values,
        ILogger<KafkaSource<TKey, TEntity>>? logger = null)
    {
        (_config, _topic, _keyDeserializer, _valueDeserializer) = (config, topic, keys, values);
        _logger = logger ?? NullLogger<KafkaSource<TKey, TEntity>>.Instance;
        KafkaTelemetry.Observe(this, new KafkaTelemetry.Lag(typeof(TEntity).Name, topic, _lag));
    }

    public async IAsyncEnumerable<Change<TKey, TEntity>> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Consume blocks: leave the caller's thread first.
        await Task.Yield();
        // GroupId only because the client requires one; nothing is committed under it.
        var settings = new ConsumerConfig(new Dictionary<string, string>(_config))
        {
            GroupId = _config.GroupId ?? $"indexed-cache-{Guid.NewGuid()}", EnableAutoCommit = false, EnablePartitionEof = true
        };
        using var consumer = new ConsumerBuilder<TKey, byte[]?>(settings)
            .SetKeyDeserializer(_keyDeserializer)
            .SetValueDeserializer(Deserializers.ByteArray!)
            .SetLogHandler((_, m) => _logger.Log(LevelOf(m.Level), "Kafka {Facility} ({Topic}): {Message}", m.Facility, _topic, m.Message))
            .SetErrorHandler((_, e) => _logger.Log(e.IsFatal ? LogLevel.Error : LogLevel.Warning, "Kafka error ({Topic}): {Reason} ({Code})", _topic, e.Reason, e.Code))
            .Build();
        using var admin = new DependentAdminClientBuilder(consumer.Handle).Build();
        var metadata = admin.GetMetadata(_topic, Timeout).Topics.Single();
        if (metadata.Error.IsError) throw new KafkaException(metadata.Error);

        var partitions = metadata.Partitions.Select(p => new TopicPartition(_topic, p.PartitionId)).ToList();
        var start = partitions.Select(p => (Partition: p.Partition.Value, Offsets: consumer.QueryWatermarkOffsets(p, Timeout))).ToList();
        try
        {
            foreach (var (partition, offsets) in start) _lag[partition] = offsets.High - offsets.Low;
            _logger.LogInformation("Reading {Topic}: {Partitions} partitions, {Messages} messages to hydrate", _topic, partitions.Count, _lag.Values.Sum());
            var tracker = new CaughtUpTracker(start.Select(x => (x.Partition, x.Offsets.Low.Value, x.Offsets.High.Value)));
            consumer.Assign(partitions.Select(p => new TopicPartitionOffset(p, Offset.Beginning)));
            if (tracker.CaughtUp) yield return CaughtUp();

            while (true)
            {
                var result = consumer.Consume(cancellationToken);
                var wasCaughtUp = tracker.CaughtUp;
                if (result.IsPartitionEOF)
                {
                    tracker.Eof(result.Partition.Value);
                    TrackLag(consumer, result.TopicPartition, next: result.Offset);
                }
                else
                {
                    tracker.Consumed(result.Partition.Value, result.Offset.Value);
                    TrackLag(consumer, result.TopicPartition, next: result.Offset + 1);
                    var key = result.Message.Key ?? throw new InvalidDataException($"The message at {result.TopicPartitionOffset} has no key.");
                    yield return result.Message.Value is null
                        ? new Change<TKey, TEntity>.Delete(key)
                        : new Change<TKey, TEntity>.Upsert(key, _valueDeserializer.Deserialize(result.Message.Value, isNull: false,
                            new SerializationContext(MessageComponentType.Value, _topic, result.Message.Headers)));
                }
                if (!wasCaughtUp && tracker.CaughtUp) yield return CaughtUp();
            }
        }
        finally
        {
            _lag.Clear();
        }
    }

    private Change<TKey, TEntity> CaughtUp()
    {
        _logger.LogInformation("Caught up with {Topic}", _topic);
        return new Change<TKey, TEntity>.CaughtUp();
    }

    // The client's cached high watermark (from its fetches): no broker call.
    private void TrackLag(IConsumer<TKey, byte[]?> consumer, TopicPartition partition, long next)
    {
        var high = consumer.GetWatermarkOffsets(partition).High;
        if (high != Offset.Unset) _lag[partition.Partition.Value] = Math.Max(0, high - next);
    }

    private static LogLevel LevelOf(SyslogLevel level) => level switch
    {
        <= SyslogLevel.Error => LogLevel.Error,
        SyslogLevel.Warning => LogLevel.Warning,
        SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
        _ => LogLevel.Debug,
    };
}

internal static class KafkaTelemetry
{
    // The same meter name as the cache's, so one AddMeter(CacheTelemetry.Name) covers both.
    private static readonly Meter Meter = new(CacheTelemetry.Name);
    // Weak: a source that is no longer referenced stops being reported.
    private static readonly ConditionalWeakTable<object, Lag> Sources = new();

    static KafkaTelemetry() => Meter.CreateObservableGauge("indexed_cache.kafka.lag", () =>
            from source in Sources
            from partition in source.Value.Partitions
            select new Measurement<long>(partition.Value, 
                new("cache.name", source.Value.Cache), 
                new("messaging.destination.name", source.Value.Topic),
                new("messaging.destination.partition.id", partition.Key.ToString())),
        "{message}", "Offsets in a partition not read yet (transaction markers count, as in Kafka's consumer lag); while hydrating, the ones left to hydrate.");

    public static void Observe(object source, Lag lag) => Sources.Add(source, lag);

    public sealed record Lag(string Cache, string Topic, ConcurrentDictionary<int, long> Partitions);
}

// Which partitions have delivered everything they held when the source started (their high watermark then).
// A partition is caught up at a message at High - 1 or later, or at its end (EOF): the offsets before High may end with
// transaction markers, which are never delivered, and an empty partition (Low == High) never delivers anything.
internal sealed class CaughtUpTracker(IEnumerable<(int Partition, long Low, long High)> start)
{
    private readonly Dictionary<int, long> _behind = start.Where(x => x.High > x.Low).ToDictionary(x => x.Partition, x => x.High);

    public bool CaughtUp => _behind.Count == 0;

    public void Consumed(int partition, long offset)
    {
        if (_behind.TryGetValue(partition, out var high) && offset + 1 >= high) _behind.Remove(partition);
    }

    public void Eof(int partition) => _behind.Remove(partition);
}

public static class KafkaCacheBuilderExtensions
{
    public static CacheBuilder<TKey, TEntity> FromKafka<TKey, TEntity>(this CacheBuilder<TKey, TEntity> builder,
        ConsumerConfig config, string topic, IDeserializer<TKey> keys, IDeserializer<TEntity> values) where TKey : notnull where TEntity : class =>
        builder.FromSource(sp => new KafkaSource<TKey, TEntity>(config, topic, keys, values, sp.GetService<ILogger<KafkaSource<TKey, TEntity>>>()));
}
