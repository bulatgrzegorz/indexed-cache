using System.Threading.Channels;

namespace IndexedCache;

// What a cache is fed: every entity that existed when the source started, then CaughtUp once, then live changes.
// The cache is not readable until CaughtUp.
public interface ICacheSource<TKey, TEntity>
{
    IAsyncEnumerable<Change<TKey, TEntity>> ReadAsync(CancellationToken cancellationToken);
}

public abstract record Change<TKey, TEntity>
{
    private Change() { }

    public sealed record Upsert(TKey Key, TEntity Entity) : Change<TKey, TEntity>;

    public sealed record Delete(TKey Key) : Change<TKey, TEntity>;

    // Everything that existed when the source started has been delivered.
    public sealed record CaughtUp : Change<TKey, TEntity>;
}

public sealed class ChannelSource<TKey, TEntity>(ChannelReader<Change<TKey, TEntity>> reader) : ICacheSource<TKey, TEntity>
{
    public IAsyncEnumerable<Change<TKey, TEntity>> ReadAsync(CancellationToken cancellationToken) => reader.ReadAllAsync(cancellationToken);
}
