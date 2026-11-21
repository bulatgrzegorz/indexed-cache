using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Logging.Testing;
using Testcontainers.Kafka;

namespace IndexedCache.Tests;

public class CaughtUpTrackerTests
{
    [Fact]
    public void EmptyPartitionsAreCaughtUpAtOnce() => Assert.True(new CaughtUpTracker([(0, 0, 0), (1, 500, 500)]).CaughtUp);

    [Fact]
    public void CaughtUpOnceEveryPartitionReachesItsStartHigh()
    {
        var tracker = new CaughtUpTracker([(0, 0, 10), (1, 5, 8), (2, 3, 3)]);
        tracker.Consumed(0, 9);
        tracker.Consumed(1, 6);
        Assert.False(tracker.CaughtUp);
        tracker.Consumed(1, 7);
        Assert.True(tracker.CaughtUp);
    }

    // Offsets 8 and 9 are transaction markers, never delivered: the partition's end (EOF) catches it up.
    [Fact]
    public void EofCatchesUpAPartitionEndingInMarkers()
    {
        var tracker = new CaughtUpTracker([(0, 0, 10)]);
        tracker.Consumed(0, 7);
        Assert.False(tracker.CaughtUp);
        tracker.Eof(0);
        Assert.True(tracker.CaughtUp);
    }
}

public class KafkaSourceTests
{
    private sealed record Fruit(string Name);

    private sealed class FruitsByName : TextIndex<Fruit>
    {
        protected override string KeyOf(Fruit x) => x.Name;
    }

    private sealed class FruitDeserializer : IDeserializer<Fruit>
    {
        public Fruit Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) => new(Encoding.UTF8.GetString(data));
    }

    [DockerFact]
    public async Task HydratesFromTheTopicThenFollowsIt()
    {
        await using var kafka = new KafkaBuilder("confluentinc/cp-kafka:7.5.0").Build();
        await kafka.StartAsync();
        var bootstrap = kafka.GetBootstrapAddress();
        // Created up front, as the cache expects it (compacted), and on 2 partitions so CaughtUp waits for both. Left to auto-create
        // on the first produce, it sometimes never got a leader (~1 run in 6): the produce timed out.
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
            await admin.CreateTopicsAsync([new TopicSpecification { Name = "fruits", NumPartitions = 2, ReplicationFactor = 1, Configs = new() { ["cleanup.policy"] = "compact" } }]);
        // 30 s, not the default 5 min: a broken broker fails the test quickly. Transactional, so each partition written to ends in
        // a commit marker the consumer never receives: only the partition's end (EOF) tells the source it caught up, and clears its lag.
        using var producer = new ProducerBuilder<string, string?>(new ProducerConfig { BootstrapServers = bootstrap, MessageTimeoutMs = 30_000, TransactionalId = "fruits-test" }).Build();
        producer.InitTransactions(TimeSpan.FromSeconds(30));
        await Produce(producer, ("K1", "APPLE"), ("K2", "BANANA"), ("K2", null), ("K3", "CHERRY"));

        using var lag = new MetricCollector<long>(null, CacheTelemetry.Name, "indexed_cache.kafka.lag");
        var logger = new FakeLogger<KafkaSource<string, Fruit>>();
        var byName = new FruitsByName();
        var cache = new Cache<string, Fruit>([byName]);
        using var stop = new CancellationTokenSource();
        var run = cache.RunAsync(new KafkaSource<string, Fruit>(new ConsumerConfig { BootstrapServers = bootstrap }, "fruits", Deserializers.Utf8, new FruitDeserializer(), logger), stop.Token);
        await cache.WaitUntilReadyAsync().WaitAsync(TimeSpan.FromSeconds(60));

        Assert.Equal("APPLE", cache.Get("K1")?.Name);
        Assert.Null(cache.Get("K2"));
        Assert.Equal(["APPLE", "CHERRY"], byName.StartsWith("").Select(x => x.Name).Order());

        await Produce(producer, ("K1", "AVOCADO"));
        await Until(() => cache.Get("K1")?.Name == "AVOCADO");
        Assert.Equal("AVOCADO", Assert.Single(byName.StartsWith("AV")).Name);
        // The marker after AVOCADO counts until the partition's EOF is read.
        await Until(() => Lag(lag) is [("0", 0), ("1", 0)]);
        var logs = logger.Collector.GetSnapshot().Select(l => l.Message).ToList();
        // Offsets, as Kafka counts lag: the 4 messages and their transaction's commit marker (the keys all land in one partition).
        Assert.Contains("Reading fruits: 2 partitions, 5 messages to hydrate", logs);
        Assert.Contains("Caught up with fruits", logs);

        await stop.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
        Assert.Empty(Lag(lag)); // not reading: no lag reported
        await byName.DisposeAsync();
    }

    private static async Task Produce(IProducer<string, string?> producer, params (string Key, string? Value)[] messages)
    {
        producer.BeginTransaction();
        foreach (var (key, value) in messages) await producer.ProduceAsync("fruits", new Message<string, string?> { Key = key, Value = value });
        producer.CommitTransaction();
    }

    private static Task Until(Func<bool> condition) => Task.Run(async () =>
    {
        while (!condition()) await Task.Delay(50);
    }).WaitAsync(TimeSpan.FromSeconds(30));

    private static List<(string Partition, long Lag)> Lag(MetricCollector<long> lag)
    {
        lag.Clear();
        lag.RecordObservableInstruments();
        return [.. lag.GetMeasurementSnapshot()
            .Where(m => (string?)m.Tags["messaging.destination.name"] == "fruits")
            .Select(m => ((string)m.Tags["messaging.destination.partition.id"]!, m.Value))
            .Order()];
    }
}

public sealed class DockerFactAttribute : FactAttribute
{
    public DockerFactAttribute()
    {
        if (!File.Exists("/var/run/docker.sock") && Environment.GetEnvironmentVariable("DOCKER_HOST") is null) Skip = "Needs Docker (Testcontainers).";
    }
}
