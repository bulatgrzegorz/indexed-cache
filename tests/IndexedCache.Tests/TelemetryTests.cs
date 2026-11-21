using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using OpenTelemetry;
using OpenTelemetry.Metrics;

namespace IndexedCache.Tests;

// Each test has its own entity type: its cache.name tells its measurements and spans apart from other tests' running in parallel.
public sealed record Probe(string? Name, Kind Kind);
public sealed class ProbesByName : TextIndex<Probe> { protected override string? KeyOf(Probe x) => x.Name; }
public sealed class ProbesByKind : ExactIndex<Probe, Kind> { protected override Kind ValueOf(Probe x) => x.Kind; }

public sealed record Faulty(string? Name);
public sealed class FaultyByName : TextIndex<Faulty> { protected override string? KeyOf(Faulty x) => x.Name; }

public sealed record Exported(string? Name);
public sealed class ExportedByName : TextIndex<Exported> { protected override string? KeyOf(Exported x) => x.Name; }

public class TelemetryTests
{
    [Fact]
    public async Task ReportsEveryChangeQueryStateSpanAndLog()
    {
        using var changes = Collect<double>("indexed_cache.change.duration");
        using var updates = Collect<double>("indexed_cache.index.update.duration");
        using var builds = Collect<double>("indexed_cache.index.build.duration");
        using var hydrations = Collect<double>("indexed_cache.hydration.duration");
        using var queries = Collect<double>("indexed_cache.query.duration");
        using var lookups = Collect<long>("indexed_cache.lookups");
        using var status = Collect<int>("indexed_cache.status");
        using var entities = Collect<int>("indexed_cache.entities");
        using var entries = Collect<int>("indexed_cache.index.entries");
        using var age = Collect<double>("indexed_cache.last_change.age");
        var spans = new List<Activity>();
        using var listener = Listen(spans, "Probe");
        var logger = new FakeLogger<Cache<string, Probe>>();
        var (byName, byKind) = (new ProbesByName(), new ProbesByKind());
        var cache = new Cache<string, Probe>([byName, byKind], logger);

        Assert.Equal(new Dictionary<string, int> { ["hydrating"] = 1, ["ready"] = 0, ["faulted"] = 0 }, Statuses(status, "Probe"));
        // So the age since the cache was created and the age since its last change differ by at least this much.
        await Task.Delay(200);
        var run = Stopwatch.GetTimestamp();

        await cache.RunAsync(Source<Probe>(
            new Change<string, Probe>.Upsert("K1", new("APPLE", Kind.Stock)),
            new Change<string, Probe>.Upsert("K2", new("BANANA", Kind.Bond)),
            new Change<string, Probe>.Delete("K2"),
            new Change<string, Probe>.CaughtUp(),
            new Change<string, Probe>.Upsert("K1", new("APRICOT", Kind.Stock)),
            new Change<string, Probe>.Upsert("K3", new(null, Kind.Fund)),
            new Change<string, Probe>.Delete("K9")), CancellationToken.None);
        byName.StartsWith("AP");
        byName.EndsWith("T");
        byName.Contains("RI");
        byKind.Get(Kind.Fund);
        cache.Get("K1");
        cache.Get("K9");

        Assert.Equal(
            [("insert", "hydrating"), ("insert", "hydrating"), ("delete", "hydrating"), ("update", "live"), ("insert", "live"), ("delete_missing", "live")],
            Of(changes, "Probe").Select(m => ((string)m.Tags["cache.change"]!, (string)m.Tags["cache.phase"]!)));
        // update and insert, each to both indexes; delete_missing touches none
        Assert.Equal(["ProbesByKind", "ProbesByKind", "ProbesByName", "ProbesByName"], Of(updates, "Probe").Select(m => (string)m.Tags["cache.index"]!).Order());
        Assert.Equal(["ProbesByKind", "ProbesByName"], Of(builds, "Probe").Select(m => (string)m.Tags["cache.index"]!).Order());
        Assert.Single(Of(hydrations, "Probe"));
        Assert.Equal([("ProbesByKind", "get"), ("ProbesByName", "contains"), ("ProbesByName", "ends_with"), ("ProbesByName", "starts_with")],
            Of(queries, "Probe").Select(m => ((string)m.Tags["cache.index"]!, (string)m.Tags["cache.query"]!)).Order());
        Assert.Equal([false, true], Of(lookups, "Probe").Select(m => (bool)m.Tags["cache.found"]!).Order());
        Assert.All(Of(changes, "Probe").Concat(Of(queries, "Probe")), m => Assert.InRange(m.Value, 0, 1));

        Assert.Equal(new Dictionary<string, int> { ["hydrating"] = 0, ["ready"] = 1, ["faulted"] = 0 }, Statuses(status, "Probe"));
        Assert.Equal(2, Gauge(entities, "Probe").Single().Value);
        // K3 has no name
        Assert.Equal([("ProbesByKind", 2), ("ProbesByName", 1)], Gauge(entries, "Probe").Select(m => ((string)m.Tags["cache.index"]!, m.Value)).Order());
        Assert.InRange(Gauge(age, "Probe").Single().Value, 0, Stopwatch.GetElapsedTime(run).TotalSeconds);

        var hydrate = Assert.Single(spans, s => s.OperationName == "indexed_cache.hydrate");
        Assert.Equal(ActivityStatusCode.Unset, hydrate.Status);
        Assert.Equal(1, hydrate.GetTagItem("cache.entities"));
        Assert.Equal(3, hydrate.GetTagItem("cache.changes"));
        var indexSpans = spans.Where(s => s.OperationName == "indexed_cache.build_index").ToList();
        Assert.Equal(["ProbesByKind", "ProbesByName"], indexSpans.Select(s => (string)s.GetTagItem("cache.index")!).Order());
        Assert.All(indexSpans, s => Assert.Equal(hydrate.SpanId, s.ParentSpanId));

        var logs = logger.Collector.GetSnapshot();
        Assert.All(logs, l => Assert.Equal(LogLevel.Information, l.Level));
        Assert.Collection(logs.Select(l => l.Message).Order(StringComparer.Ordinal),
            m => Assert.Equal("Cache Probe built index ProbesByKind: 1 entities", m[..m.IndexOf(" in ", StringComparison.Ordinal)]),
            m => Assert.Equal("Cache Probe built index ProbesByName: 1 entities", m[..m.IndexOf(" in ", StringComparison.Ordinal)]),
            m => Assert.Equal("Cache Probe hydrating", m),
            m => Assert.StartsWith("Cache Probe ready: 1 entities from 3 changes in ", m),
            m => Assert.Equal("Cache Probe: its source ended", m));
    }

    [Fact]
    public async Task FaultIsAnErrorLogAFailedSpanAndAStatus()
    {
        using var status = Collect<int>("indexed_cache.status");
        var spans = new List<Activity>();
        using var listener = Listen(spans, "Faulty");
        var logger = new FakeLogger<Cache<string, Faulty>>();
        var cache = new Cache<string, Faulty>([new FaultyByName()], logger);
        var channel = Channel.CreateUnbounded<Change<string, Faulty>>();
        channel.Writer.TryWrite(new Change<string, Faulty>.Upsert("K1", new("APPLE")));
        channel.Writer.Complete(new IOException("broker gone"));

        await Assert.ThrowsAnyAsync<Exception>(() => cache.RunAsync(new ChannelSource<string, Faulty>(channel.Reader), CancellationToken.None));

        Assert.Equal(new Dictionary<string, int> { ["hydrating"] = 0, ["ready"] = 0, ["faulted"] = 1 }, Statuses(status, "Faulty"));
        var hydrate = Assert.Single(spans, s => s.OperationName == "indexed_cache.hydrate");
        Assert.Equal(ActivityStatusCode.Error, hydrate.Status);
        Assert.Contains(hydrate.Events, e => e.Name == "exception");
        var error = logger.LatestRecord;
        Assert.Equal(LogLevel.Error, error.Level);
        Assert.Equal("Cache Faulty faulted while hydrating: it takes no more changes", error.Message);
        Assert.NotNull(error.Exception);
    }

    // Through the OpenTelemetry SDK, as an app would wire it: every instrument exported, durations in the advised (seconds) buckets.
    [Fact]
    public async Task OpenTelemetryExportsEveryInstrument()
    {
        var exported = new List<Metric>();
        using (var meters = Sdk.CreateMeterProviderBuilder().AddMeter(CacheTelemetry.Name).AddInMemoryExporter(exported).Build())
        {
            var byName = new ExportedByName();
            var cache = new Cache<string, Exported>([byName]);
            await cache.RunAsync(Source<Exported>(
                new Change<string, Exported>.Upsert("K1", new("APPLE")),
                new Change<string, Exported>.CaughtUp(),
                new Change<string, Exported>.Upsert("K2", new("APRICOT"))), CancellationToken.None);
            byName.StartsWith("AP");
            cache.Get("K1");
            meters.ForceFlush();

            AssertExported(exported);
        }
    }

    private static void AssertExported(List<Metric> exported)
    {
        Assert.Superset(new HashSet<string>
        {
            "indexed_cache.change.duration", "indexed_cache.index.update.duration", "indexed_cache.index.build.duration", "indexed_cache.hydration.duration",
            "indexed_cache.query.duration", "indexed_cache.lookups", "indexed_cache.status", "indexed_cache.entities", "indexed_cache.index.entries",
            "indexed_cache.last_change.age",
        }, exported.Select(m => m.Name).ToHashSet());
        var query = exported.First(m => m.Name == "indexed_cache.query.duration");
        Assert.Equal("s", query.Unit);
        foreach (ref readonly var point in query.GetMetricPoints())
        {
            var bounds = new List<double>();
            foreach (var bucket in point.GetHistogramBuckets()) bounds.Add(bucket.ExplicitBound);
            Assert.Equal(1e-6, bounds[0]);
        }
    }

    private static MetricCollector<T> Collect<T>(string instrument) where T : struct => new(null, CacheTelemetry.Name, instrument);

    private static IEnumerable<CollectedMeasurement<T>> Of<T>(MetricCollector<T> collector, string cache) where T : struct =>
        collector.GetMeasurementSnapshot().Where(m => m.Tags.TryGetValue("cache.name", out var name) && (string?)name == cache);

    private static IEnumerable<CollectedMeasurement<T>> Gauge<T>(MetricCollector<T> collector, string cache) where T : struct
    {
        collector.Clear();
        collector.RecordObservableInstruments();
        return Of(collector, cache);
    }

    private static Dictionary<string, int> Statuses(MetricCollector<int> status, string cache) =>
        Gauge(status, cache).ToDictionary(m => (string)m.Tags["cache.status"]!, m => m.Value);

    private static ICacheSource<string, T> Source<T>(params Change<string, T>[] changes)
    {
        var channel = Channel.CreateUnbounded<Change<string, T>>();
        foreach (var change in changes) channel.Writer.TryWrite(change);
        channel.Writer.Complete();
        return new ChannelSource<string, T>(channel.Reader);
    }

    // Other tests' caches report too: keeps only the spans of this test's cache.
    private static ActivityListener Listen(List<Activity> spans, string cache)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == CacheTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a =>
            {
                if ((string?)a.GetTagItem("cache.name") != cache) return;
                lock (spans) spans.Add(a);
            },
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
