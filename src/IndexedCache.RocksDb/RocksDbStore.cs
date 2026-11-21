using System.Buffers.Binary;
using System.Collections;
using System.Text.Json;
using RocksDbSharp;

namespace IndexedCache;

// Entities in RocksDB, keyed by id (4 bytes big-endian, so they iterate in id order). The managed heap keeps the cache's
// key -> id map and one bit per id here; the rest is RocksDB's (memtables, block cache, files).
// Every read is a Get and a deserialize: Cache.Get, each result item as it is read, the old version on each live change,
// and every entity once per index at the build.
// Every start rehydrates, so the database is a new folder under directory (default: the temp folder), without a WAL,
// deleted on Dispose.
// shortcut: a removed entity's last version stays on disk until restart (results being read still resolve it), as
// InMemoryStore keeps it in memory; a crashed process leaves its folder behind.
public sealed class RocksDbStore<TEntity> : IEntityStore<TEntity>, IDisposable where TEntity : class
{
    private readonly string _path;
    private readonly RocksDb _db;
    private readonly WriteOptions _write = new WriteOptions().DisableWal(1);
    private readonly Func<TEntity, byte[]> _serialize;
    private readonly Deserializer _deserializer;
    // Writer only; read by All, while nothing writes.
    private readonly BitArray _live = new(16);
    private int _disposed;

    // JSON (System.Text.Json) unless serialize / deserialize are given.
    public RocksDbStore(string? directory = null, Func<TEntity, byte[]>? serialize = null, Func<ReadOnlySpan<byte>, TEntity>? deserialize = null)
    {
        _path = Path.Combine(directory ?? Path.GetTempPath(), $"indexed-cache-{typeof(TEntity).Name}-{Guid.NewGuid():N}");
        _db = RocksDb.Open(new DbOptions().SetCreateIfMissing(true), _path);
        _serialize = serialize ?? (x => JsonSerializer.SerializeToUtf8Bytes(x));
        _deserializer = new Deserializer(deserialize ?? (x => JsonSerializer.Deserialize<TEntity>(x)!));
    }

    public TEntity this[int id]
    {
        get
        {
            Span<byte> key = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(key, id);
            return _db.Get(key, _deserializer);
        }
    }

    public IEnumerable<(int Id, TEntity Entity)> All
    {
        get
        {
            using var iterator = _db.NewIterator();
            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            {
                var id = BinaryPrimitives.ReadInt32BigEndian(iterator.GetKeySpan());
                if (_live[id]) yield return (id, _deserializer.Deserialize(iterator.GetValueSpan()));
            }
        }
    }

    public void Set(int id, TEntity entity)
    {
        Span<byte> key = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, id);
        _db.Put(key, _serialize(entity), writeOptions: _write);
        if (id >= _live.Length) _live.Length = Math.Max(id + 1, _live.Length * 2);
        _live[id] = true;
    }

    public void Remove(int id) => _live[id] = false;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        _db.Dispose();
        Directory.Delete(_path, recursive: true);
    }

    private sealed class Deserializer(Func<ReadOnlySpan<byte>, TEntity> deserialize) : ISpanDeserializer<TEntity>
    {
        public TEntity Deserialize(ReadOnlySpan<byte> buffer) => deserialize(buffer);
    }
}

public static class RocksDbCacheBuilderExtensions
{
    // The entities in a RocksDbStore, deleted when the container is disposed.
    public static CacheBuilder<TKey, TEntity> UseRocksDb<TKey, TEntity>(this CacheBuilder<TKey, TEntity> builder, string? directory = null,
        Func<TEntity, byte[]>? serialize = null, Func<ReadOnlySpan<byte>, TEntity>? deserialize = null) where TKey : notnull where TEntity : class =>
        builder.UseStore(_ => new RocksDbStore<TEntity>(directory, serialize, deserialize));
}
