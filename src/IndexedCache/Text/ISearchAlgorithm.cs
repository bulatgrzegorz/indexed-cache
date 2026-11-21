namespace SearchAlgorithms;

// What queries match (Key) and what the caller gets back to find its entity by (Id).
// An entry is both: two entities may share a Key (two companies with one name), one entity may have several Keys (aliases).
public readonly record struct Entry(string Key, int Id);

// Items are a set of unique entries, received once in the constructor (build any index there).
// Adding an existing entry or removing a missing one does nothing.
// Queries match Keys, ordinal (case-sensitive), and result order is free.
// All members may be called concurrently from many threads (searches while data changes); DisposeAsync is called once, after all of them.
public interface ISearchAlgorithm : IAsyncDisposable
{
    IReadOnlyList<Entry> GetByStartsWith(string prefix);
    IReadOnlyList<Entry> GetByEndsWith(string suffix);
    IReadOnlyList<Entry> Filter(string fragment);
    void Add(Entry item);
    void Remove(Entry item);
}
