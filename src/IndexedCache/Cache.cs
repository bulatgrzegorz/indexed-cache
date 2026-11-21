using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TelemetryTag = System.Collections.Generic.KeyValuePair<string, object?>;
// ReSharper disable StaticMemberInGenericType

namespace IndexedCache;

public enum CacheStatus { Hydrating, Ready, Faulted }

// Entities by key, plus the indexes over them, fed by one source (RunAsync).
// Until the source's CaughtUp, changes only go into the store; then every index is built once from all of it
// (one bulk build is far cheaper than an Add per entity), and from there each change updates every index.
// Reads may come from any thread at any time. A write updates the store, then the indexes one by one, so for a moment a hit
// may resolve to a newer version of its entity than the one that matched, or an index may miss an entity being changed.
// Telemetry (CacheTelemetry): metrics tagged cache.name (TEntity's name), a span over hydration with one per index build, lifecycle logs.
public sealed class Cache<TKey, TEntity> : IObservedCache where TKey : notnull where TEntity : class
{
    private static readonly string Name = typeof(TEntity).Name;

    private static readonly TelemetryTag Hydrating = Telemetry.Tag("cache.phase", "hydrating"); 
    private static readonly TelemetryTag Live = Telemetry.Tag("cache.phase", "live");

    private static readonly TelemetryTag Inserted = Telemetry.Tag("cache.change", "insert");
    private static readonly TelemetryTag Updated = Telemetry.Tag("cache.change", "update");
    private static readonly TelemetryTag Deleted = Telemetry.Tag("cache.change", "delete");
    private static readonly TelemetryTag DeletedMissing = Telemetry.Tag("cache.change", "delete_missing");

    private static readonly TelemetryTag Found = Telemetry.Tag("cache.found", true); 
    private static readonly TelemetryTag Missing = Telemetry.Tag("cache.found", false);

    private readonly ICacheIndex<TEntity>[] _indexes;
    private readonly TelemetryTag[] _indexTags;
    private readonly ILogger _logger;
    // Keys stay in memory whatever the store: index entries name entities by id, and the store holds them by id.
    private readonly ConcurrentDictionary<TKey, int> _ids = new();
    private readonly IEntityStore<TEntity> _store;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private volatile CacheStatus _status;
    private volatile bool _built;
    private int _running;
    private int _count;
    private int _nextId;
    private long _lastChange = Stopwatch.GetTimestamp();
    // Hydration so far, for its span, metric and log.
    private Activity? _hydration;
    private long _hydrationStart;
    private int _hydrationChanges;

    public Cache(IEnumerable<ICacheIndex<TEntity>> indexes, ILogger<Cache<TKey, TEntity>>? logger = null, IEntityStore<TEntity>? store = null)
    {
        _indexes = [.. indexes];
        _store = store ?? new InMemoryStore<TEntity>();
        _indexTags = [.. _indexes.Select(Telemetry.IndexTag)];
        _logger = logger ?? NullLogger<Cache<TKey, TEntity>>.Instance;
        Telemetry.Observe(this);
    }

    public CacheStatus Status => _status;

    public int Count => Volatile.Read(ref _count);

    public Task WaitUntilReadyAsync(CancellationToken cancellationToken = default) => _ready.Task.WaitAsync(cancellationToken);

    public TEntity? Get(TKey key)
    {
        if (!_built) throw Errors.NotHydrated();
        var found = _ids.TryGetValue(key, out var id);
        Telemetry.Lookups.Add(1, CacheTag<TEntity>.Name, found ? Found : Missing);
        return found ? _store[id] : null;
    }

    // The only writer: applies the source's changes until it ends or cancellationToken fires.
    // A failure leaves the cache Faulted (still readable, if it was) and is rethrown.
    public async Task RunAsync(ICacheSource<TKey, TEntity> source, CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _running, 1) == 1) 
            throw new InvalidOperationException("The cache is already running: it has one source and one writer.");
        
        _logger.LogInformation("Cache {Cache} hydrating", Name);
        _hydrationStart = Stopwatch.GetTimestamp();
        _hydration = Telemetry.Source.StartActivity("indexed_cache.hydrate")?.SetTag(CacheTag<TEntity>.Name.Key, Name);
        
        try
        {
            await foreach (var change in source.ReadAsync(cancellationToken)) Apply(change);
            
            if (!_built) throw new InvalidOperationException("The source ended before it caught up.");
            
            _logger.LogInformation("Cache {Cache}: its source ended", Name);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            EndHydration(error: null);
            
            _logger.LogInformation("Cache {Cache} stopped", Name);
            throw;
        }
        catch (Exception e)
        {
            _status = CacheStatus.Faulted;
            
            EndHydration(e);
            
            _logger.LogError(e, "Cache {Cache} faulted while {Phase}: it takes no more changes", Name, _built ? Live.Value : Hydrating.Value);
            _ready.TrySetException(e);
            throw;
        }
    }

    private void Apply(Change<TKey, TEntity> change)
    {
        if (change is Change<TKey, TEntity>.CaughtUp)
        {
            if (!_built) BuildIndexes();
            return;
        }
        var start = Stopwatch.GetTimestamp();
        var kind = change switch
        {
            Change<TKey, TEntity>.Upsert(var key, var entity) => Upsert(key, entity),
            Change<TKey, TEntity>.Delete(var key) => Delete(key),
            _ => throw new UnreachableException(),
        };
        var end = Stopwatch.GetTimestamp();
        Volatile.Write(ref _lastChange, end);
        if (!_built) _hydrationChanges++;
        Telemetry.ChangeDuration.Record(Stopwatch.GetElapsedTime(start, end).TotalSeconds, CacheTag<TEntity>.Name, kind, _built ? Live : Hydrating);
    }

    private TelemetryTag Upsert(TKey key, TEntity entity)
    {
        // The old version only for the indexes: from a store on disk, reading it is a lookup and a deserialize.
        if (_ids.TryGetValue(key, out var id))
        {
            var old = _built ? _store[id] : null;
            _store.Set(id, entity);
            Update(id, old, entity);
            return Updated;
        }
        id = _nextId++;
        _store.Set(id, entity);
        _ids[key] = id;
        Volatile.Write(ref _count, _count + 1);
        Update(id, null, entity);
        return Inserted;
    }

    private TelemetryTag Delete(TKey key)
    {
        if (!_ids.TryGetValue(key, out var id)) return DeletedMissing;
        if (_built) Update(id, _store[id], null);
        _ids.TryRemove(key, out _);
        _store.Remove(id);
        Volatile.Write(ref _count, _count - 1);
        return Deleted;
    }

    // Before the build there is nothing to update: the build reads every entity as it is then.
    private void Update(int id, TEntity? old, TEntity? @new)
    {
        if (!_built) return;
        for (var i = 0; i < _indexes.Length; i++)
        {
            var start = Stopwatch.GetTimestamp();
            _indexes[i].Update(id, old, @new);
            Telemetry.IndexUpdateDuration.Record(Stopwatch.GetElapsedTime(start).TotalSeconds, CacheTag<TEntity>.Name, _indexTags[i]);
        }
    }

    private void BuildIndexes()
    {
        Parallel.For(0, _indexes.Length, BuildIndex);
        
        _built = true;
        _status = CacheStatus.Ready;
        
        var elapsed = Stopwatch.GetElapsedTime(_hydrationStart);
        Telemetry.HydrationDuration.Record(elapsed.TotalSeconds, CacheTag<TEntity>.Name);
        _hydration?.SetTag("cache.entities", _count).SetTag("cache.changes", _hydrationChanges);
        
        EndHydration(error: null);
        
        _logger.LogInformation("Cache {Cache} ready: {Entities} entities from {Changes} changes in {ElapsedMs:F0} ms",
            Name, _count, _hydrationChanges, elapsed.TotalMilliseconds);
        
        _ready.TrySetResult();
    }

    // In parallel; each span a child of the hydration's (Activity.Current flows into Parallel's tasks).
    private void BuildIndex(int i)
    {
        var (index, tag) = (_indexes[i], _indexTags[i]);
        using var activity = Telemetry.Source.StartActivity("indexed_cache.build_index")?.SetTag(CacheTag<TEntity>.Name.Key, Name).SetTag(tag.Key, tag.Value);
        var start = Stopwatch.GetTimestamp();
        index.Build(_store);
        var elapsed = Stopwatch.GetElapsedTime(start);
        Telemetry.IndexBuildDuration.Record(elapsed.TotalSeconds, CacheTag<TEntity>.Name, tag);
        activity?.SetTag("cache.index.entries", index.Count);
        _logger.LogInformation("Cache {Cache} built index {Index}: {Entries} entities in {ElapsedMs:F0} ms", Name, tag.Value, index.Count, elapsed.TotalMilliseconds);
    }

    // The hydration span ends at ready, or with RunAsync if that comes first.
    private void EndHydration(Exception? error)
    {
        var activity = _hydration;
        _hydration = null;
        if (activity is null) return;
        if (error is not null) activity.SetStatus(ActivityStatusCode.Error, error.Message).AddException(error);
        activity.Stop();
    }

    TelemetryTag IObservedCache.Tag => CacheTag<TEntity>.Name;

    TimeSpan IObservedCache.SinceLastChange => Stopwatch.GetElapsedTime(Volatile.Read(ref _lastChange));

    IEnumerable<(TelemetryTag Tag, int Count)> IObservedCache.Indexes => _indexes.Select((x, i) => (_indexTags[i], x.Count));
}

internal static class Errors
{
    public static InvalidOperationException NotHydrated() => new("The cache is still hydrating: it answers once its source has caught up.");
}
