using SearchAlgorithms;

namespace IndexedCache;

// A TextIndex on Lucene.Net: n-gram terms in a temp folder, deleted when the index is disposed.
public abstract class LuceneTextIndex<TEntity> : TextIndex<TEntity> where TEntity : class
{
    protected override ISearchAlgorithm Create(Entry[] items) => new LuceneIndex(items);
}
