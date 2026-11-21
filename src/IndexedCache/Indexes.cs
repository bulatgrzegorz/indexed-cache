using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using SearchAlgorithms;

namespace IndexedCache;

// What the cache calls on an index. TextIndex and ExactIndex implement it; implement it directly for another kind of index.
// Build is called once, when hydration ends, before any Update; Update only from the cache's single writer,
// with old null for an added entity and @new null for a deleted one. Queries before Build should throw (Errors.NotHydrated).
public interface ICacheIndex<TEntity> : IAsyncDisposable where TEntity : class
{
    // Entities in the index, read by the indexed_cache.index.entries gauge from another thread.
    int Count { get; }
    void Build(IEntities<TEntity> entities);
    void Update(int id, TEntity? old, TEntity? @new);
}

// StartsWith / EndsWith / Contains over one string of each entity (KeyOf; null leaves the entity out), ordinal.
// Create picks the algorithm: SortedArrayIndex unless overridden (LuceneTextIndex).
public abstract class TextIndex<TEntity> : ICacheIndex<TEntity> where TEntity : class
{
    private readonly KeyValuePair<string, object?> _tag;
    private IEntities<TEntity> _entities = null!;
    private ISearchAlgorithm? _algorithm;
    private int _count;
    private int _disposed;

    protected TextIndex() => _tag = Telemetry.IndexTag(this);

    protected abstract string? KeyOf(TEntity entity);

    protected virtual ISearchAlgorithm Create(Entry[] items) => new SortedArrayIndex(items);

    public int Count => Volatile.Read(ref _count);

    public IReadOnlyList<TEntity> StartsWith(string prefix) => Query(Telemetry.StartsWith, prefix, static (a, q) => a.GetByStartsWith(q));

    public IReadOnlyList<TEntity> EndsWith(string suffix) => Query(Telemetry.EndsWith, suffix, static (a, q) => a.GetByEndsWith(q));

    public IReadOnlyList<TEntity> Contains(string fragment) => Query(Telemetry.Contains, fragment, static (a, q) => a.Filter(q));

    // _entities first: a reader that sees _algorithm sees it too.
    void ICacheIndex<TEntity>.Build(IEntities<TEntity> entities)
    {
        _entities = entities;
        Entry[] items = [.. from x in entities.All let key = KeyOf(x.Entity) where key is not null select new Entry(key, x.Id)];
        _count = items.Length;
        Volatile.Write(ref _algorithm, Create(items));
    }

    // Remove first: a reader may miss the entity for a moment, but never finds it twice.
    void ICacheIndex<TEntity>.Update(int id, TEntity? old, TEntity? @new)
    {
        var (from, to) = (old is null ? null : KeyOf(old), @new is null ? null : KeyOf(@new));
        if (from == to) return;
        if (from is not null) _algorithm!.Remove(new Entry(from, id));
        if (to is not null) _algorithm!.Add(new Entry(to, id));
        Volatile.Write(ref _count, _count + (to is null ? 0 : 1) - (from is null ? 0 : 1));
    }

    // Once: DI disposes an index through both its registrations (as itself and as ICacheIndex).
    public ValueTask DisposeAsync() =>
        Interlocked.Exchange(ref _disposed, 1) == 0 && _algorithm is not null ? _algorithm.DisposeAsync() : ValueTask.CompletedTask;

    private IReadOnlyList<TEntity> Query(KeyValuePair<string, object?> query, string text, Func<ISearchAlgorithm, string, IReadOnlyList<Entry>> search)
    {
        var algorithm = Volatile.Read(ref _algorithm) ?? throw Errors.NotHydrated();
        var start = Stopwatch.GetTimestamp();
        var hits = search(algorithm, text);
        Telemetry.QueryDuration.Record(Stopwatch.GetElapsedTime(start).TotalSeconds, CacheTag<TEntity>.Name, _tag, query);
        return new Resolved<Entry, TEntity>(hits, static x => x.Id, _entities);
    }
}

// Entities whose ValueOf equals a value (EqualityComparer<TValue>.Default; null leaves the entity out): a string, an enum, anything.
// Each value's ids are an immutable sorted set, so a result is a snapshot, Count is O(1) and a read O(log n).
// shortcut: ~48 B per entity (a tree node each); a sorted int[] plus a small delta, like SortedArrayIndex, if memory matters.
public abstract class ExactIndex<TEntity, TValue> : ICacheIndex<TEntity> where TEntity : class where TValue : notnull
{
    private readonly KeyValuePair<string, object?> _tag;
    private IEntities<TEntity> _entities = null!;
    private ConcurrentDictionary<TValue, ImmutableSortedSet<int>>? _ids;
    private int _count;

    protected ExactIndex() => _tag = Telemetry.IndexTag(this);

    protected abstract TValue? ValueOf(TEntity entity);

    public int Count => Volatile.Read(ref _count);

    public IReadOnlyList<TEntity> Get(TValue value)
    {
        var ids = Volatile.Read(ref _ids) ?? throw Errors.NotHydrated();
        var start = Stopwatch.GetTimestamp();
        var found = ids.TryGetValue(value, out var set);
        Telemetry.QueryDuration.Record(Stopwatch.GetElapsedTime(start).TotalSeconds, CacheTag<TEntity>.Name, _tag, Telemetry.Get);
        return found ? new Resolved<int, TEntity>(set!, static id => id, _entities) : [];
    }

    void ICacheIndex<TEntity>.Build(IEntities<TEntity> entities)
    {
        _entities = entities;
        var groups = entities.All
            .Select(x => (Found: TryValueOf(x.Entity, out var value), Value: value, x.Id))
            .Where(x => x.Found)
            .GroupBy(x => x.Value, x => x.Id)
            .Select(g => KeyValuePair.Create(g.Key, g.ToImmutableSortedSet()))
            .ToList();
        _count = groups.Sum(x => x.Value.Count);
        Volatile.Write(ref _ids, new ConcurrentDictionary<TValue, ImmutableSortedSet<int>>(groups));
    }

    // Single writer, so a plain read-then-write per value; readers keep whichever set they already took.
    void ICacheIndex<TEntity>.Update(int id, TEntity? old, TEntity? @new)
    {
        var ids = _ids!;
        var (hasFrom, hasTo) = (TryValueOf(old, out var from), TryValueOf(@new, out var to));
        if (hasFrom && hasTo && EqualityComparer<TValue>.Default.Equals(from, to)) return;
        if (hasFrom)
        {
            var left = ids[from].Remove(id);
            if (left.IsEmpty) ids.TryRemove(from, out _);
            else ids[from] = left;
        }
        if (hasTo) ids[to] = ids.TryGetValue(to, out var set) ? set.Add(id) : [id];
        Volatile.Write(ref _count, _count + (hasTo ? 1 : 0) - (hasFrom ? 1 : 0));
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    // Not `ValueOf(entity) is not null` alone: for an enum TValue, default is a real value.
    private bool TryValueOf(TEntity? entity, out TValue value)
    {
        value = default!;
        if (entity is null || ValueOf(entity) is not { } found) return false;
        value = found;
        return true;
    }
}
