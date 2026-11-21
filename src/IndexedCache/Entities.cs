using System.Collections;

namespace IndexedCache;

// What an index reads: the cache's entities by id, the int an index entry names (Entry.Id).
public interface IEntities<TEntity> where TEntity : class
{
    // Any thread. A removed id still gives its last version, so a result read after the delete still resolves.
    TEntity this[int id] { get; }

    // Every entity not removed. Only while nothing writes: the cache builds its indexes from its writer.
    IEnumerable<(int Id, TEntity Entity)> All { get; }
}

// Where the cache keeps its entities: InMemoryStore unless the builder's UseStore says otherwise (RocksDbStore, your own).
// Set and Remove come only from the cache's single writer. The cache hands out ids in order from 0 and never reuses one.
// A store the container creates is disposed with it, if it is IDisposable / IAsyncDisposable.
public interface IEntityStore<TEntity> : IEntities<TEntity> where TEntity : class
{
    // A new id, or a new version for one. The cache updates the indexes after this returns, so a reader that finds the id
    // there must get this version (or a later one) from this[id].
    void Set(int id, TEntity entity);

    // Leaves All; this[id] keeps giving the last version.
    void Remove(int id);
}

public sealed class InMemoryStore<TEntity> : IEntityStore<TEntity> where TEntity : class
{
    // Readers take it once per lookup (Volatile.Read); the writer publishes a bigger copy when it is full.
    private TEntity?[] _slots = new TEntity?[16];
    // Writer only.
    private bool[] _live = new bool[16];
    private int _next;

    // shortcut: removed entities stay in memory until restart; free slots after a grace period if deletes pile up.
    public TEntity this[int id] => Volatile.Read(ref _slots)[id]!;

    public IEnumerable<(int Id, TEntity Entity)> All
    {
        get
        {
            for (var id = 0; id < _next; id++)
                if (_live[id]) yield return (id, _slots[id]!);
        }
    }

    public void Set(int id, TEntity entity)
    {
        if (id >= _slots.Length)
        {
            var slots = _slots;
            Array.Resize(ref slots, Math.Max(id + 1, slots.Length * 2));
            Volatile.Write(ref _slots, slots);
            Array.Resize(ref _live, slots.Length);
        }
        // Volatile: a reader that finds the id in an index (updated after this) sees the entity fully built.
        Volatile.Write(ref _slots[id], entity);
        _live[id] = true;
        _next = Math.Max(_next, id + 1);
    }

    public void Remove(int id) => _live[id] = false;
}

// Index hits as entities, each looked up when read: Count and a read of the first few never touch the rest of a large result.
internal sealed class Resolved<THit, TEntity>(IReadOnlyList<THit> hits, Func<THit, int> idOf, IEntities<TEntity> entities)
    : IReadOnlyList<TEntity> where TEntity : class
{
    public int Count => hits.Count;

    public TEntity this[int index] => entities[idOf(hits[index])];

    public IEnumerator<TEntity> GetEnumerator()
    {
        foreach (var hit in hits) yield return entities[idOf(hit)];
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
