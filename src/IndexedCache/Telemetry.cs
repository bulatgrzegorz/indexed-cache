using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using TelemetryTag = System.Collections.Generic.KeyValuePair<string, object?>;

namespace IndexedCache;

// The meter and activity source every cache reports to. With OpenTelemetry:
//   .WithMetrics(m => m.AddMeter(CacheTelemetry.Name)).WithTracing(t => t.AddSource(CacheTelemetry.Name))
// Logs go to ILogger<Cache<TKey, TEntity>> (and ILogger<KafkaSource<TKey, TEntity>>).
public static class CacheTelemetry
{
    public const string Name = "IndexedCache";
}

// What the gauges read from a cache, on the collector's thread.
internal interface IObservedCache
{
    TelemetryTag Tag { get; }
    CacheStatus Status { get; }
    int Count { get; }
    TimeSpan SinceLastChange { get; }
    IEnumerable<(TelemetryTag Tag, int Count)> Indexes { get; }
}

internal static class Telemetry
{
    public static readonly ActivitySource Source = new(CacheTelemetry.Name);
    private static readonly Meter Meter = new(CacheTelemetry.Name);

    // From a query or a change (~1 us) to a hydration (a minute); OpenTelemetry's default buckets are for milliseconds.
    private static readonly InstrumentAdvice<double> Seconds = new()
    {
        HistogramBucketBoundaries = [1e-6, 2.5e-6, 5e-6, 1e-5, 2.5e-5, 5e-5, 1e-4, 2.5e-4, 5e-4, 1e-3, 2.5e-3, 5e-3, 1e-2, 2.5e-2, 5e-2, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60]
    };

    public static readonly Histogram<double> ChangeDuration = Duration("indexed_cache.change.duration", "One change from the source applied: to the store while hydrating, to the store and every index once live. cache.change: insert, update, delete, delete_missing.");
    public static readonly Histogram<double> IndexUpdateDuration = Duration("indexed_cache.index.update.duration", "One live change applied to one index.");
    public static readonly Histogram<double> IndexBuildDuration = Duration("indexed_cache.index.build.duration", "One index built from every entity, when hydration ends.");
    public static readonly Histogram<double> HydrationDuration = Duration("indexed_cache.hydration.duration", "From the first change read to the cache being ready, index builds included.");
    public static readonly Histogram<double> QueryDuration = Duration("indexed_cache.query.duration", "One index query answered (results are read lazily, so reading them is not included). cache.query: starts_with, ends_with, contains, get.");
    public static readonly Counter<long> Lookups = Meter.CreateCounter<long>("indexed_cache.lookups", "{lookup}", "Gets by key; cache.found: whether the key was there.");

    // Weak: a cache that is no longer referenced stops being reported.
    private static readonly ConditionalWeakTable<IObservedCache, object?> Caches = new();
    private static readonly (CacheStatus Status, TelemetryTag Tag)[] Statuses =
        [.. Enum.GetValues<CacheStatus>().Select(s => (s, Tag("cache.status", s.ToString().ToLowerInvariant())))];

    static Telemetry()
    {
        Meter.CreateObservableGauge("indexed_cache.status", () => from c in Observed from s in Statuses select new Measurement<int>(c.Status == s.Status ? 1 : 0, c.Tag, s.Tag), "{status}", "1 for the cache's current cache.status (hydrating, ready, faulted), 0 for the others.");
        Meter.CreateObservableGauge("indexed_cache.entities", () => from c in Observed select new Measurement<int>(c.Count, c.Tag), "{entity}", "Entities in the cache.");
        Meter.CreateObservableGauge("indexed_cache.index.entries", () => from c in Observed from i in c.Indexes select new Measurement<int>(i.Count, c.Tag, i.Tag), "{entity}", "Entities in an index: those whose indexed value is not null.");
        Meter.CreateObservableGauge("indexed_cache.last_change.age", () => from c in Observed select new Measurement<double>(c.SinceLastChange.TotalSeconds, c.Tag), "s", "Time since the last change from the source was applied (since the cache was created, before any).");
    }

    public static readonly TelemetryTag StartsWith = Tag("cache.query", "starts_with");
    public static readonly TelemetryTag EndsWith = Tag("cache.query", "ends_with");
    public static readonly TelemetryTag Contains = Tag("cache.query", "contains");
    public static readonly TelemetryTag Get = Tag("cache.query", "get");

    public static void Observe(IObservedCache cache) => Caches.Add(cache, null);

    public static TelemetryTag Tag(string name, object? value) => new(name, value);

    public static TelemetryTag IndexTag(object index) => Tag("cache.index", index.GetType().Name);

    private static IEnumerable<IObservedCache> Observed => Caches.Select(x => x.Key);

    private static Histogram<double> Duration(string name, string description) => Meter.CreateHistogram(name, "s", description, tags: null, Seconds);
}

// cache.name: the entity type's name, shared by a cache and its indexes.
internal static class CacheTag<TEntity>
{
    public static readonly TelemetryTag Name = Telemetry.Tag("cache.name", typeof(TEntity).Name);
}
