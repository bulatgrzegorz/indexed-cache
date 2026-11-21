using System.Runtime.CompilerServices;
using C = IndexedCache.Change<string, IndexedCache.Tests.Item>;

namespace IndexedCache.Tests;

public enum Kind { Stock, Bond, Fund, Option }

public sealed record Item(string Key, string? Name, string? Code, Kind Kind, string? Country);

public sealed class ByName : TextIndex<Item>
{
    protected override string? KeyOf(Item x) => x.Name;
}

public sealed class ByNameLucene : LuceneTextIndex<Item>
{
    protected override string? KeyOf(Item x) => x.Name;
}

public sealed class ByCode : TextIndex<Item>
{
    protected override string? KeyOf(Item x) => x.Code;
}

public sealed class ByKind : ExactIndex<Item, Kind>
{
    protected override Kind ValueOf(Item x) => x.Kind;
}

public sealed class ByCountry : ExactIndex<Item, string>
{
    protected override string? ValueOf(Item x) => x.Country;
}

public enum Store { InMemory, RocksDb }

// A cache with every kind of index, over the given store.
public sealed class Fixture : IAsyncDisposable
{
    public readonly ByName ByName = new();
    public readonly ByNameLucene ByNameLucene = new();
    public readonly ByCode ByCode = new();
    public readonly ByKind ByKind = new();
    public readonly ByCountry ByCountry = new();
    public readonly Cache<string, Item> Cache;
    private readonly IEntityStore<Item> _store;

    public Fixture(Store store = Store.InMemory)
    {
        _store = store == Store.RocksDb ? new RocksDbStore<Item>() : new InMemoryStore<Item>();
        Cache = new([ByName, ByNameLucene, ByCode, ByKind, ByCountry], store: _store);
    }

    // The current version of an entity: the very instance from an InMemoryStore; an equal one from a RocksDbStore, which
    // deserializes on every read.
    public void Current(Item? expected, Item? actual)
    {
        if (_store is InMemoryStore<Item>) Assert.Same(expected, actual);
        else Assert.Equal(expected, actual);
    }

    public async ValueTask DisposeAsync()
    {
        await ByName.DisposeAsync();
        await ByNameLucene.DisposeAsync();
        await ByCode.DisposeAsync();
        await ByKind.DisposeAsync();
        await ByCountry.DisposeAsync();
        (_store as IDisposable)?.Dispose();
    }
}

// Feeds a cache from a plain sequence. The sequence's code after a yield runs once the cache has applied that change
// (the cache applies each change before asking for the next), so checks placed there see exactly the changes before them.
public sealed class ScriptSource(IEnumerable<C> changes) : ICacheSource<string, Item>
{
    public async IAsyncEnumerable<C> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.Yield();
        foreach (var change in changes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return change;
        }
    }
}

// Random changes, each applied to Model as it is made. Values come from small, overlapping pools, so the hard cases are common:
// a value shared by many entities, one value a prefix / suffix / substring of another, repeated substrings ("ABAB"), "" and null,
// a value changed and changed back, a key deleted and inserted again.
public sealed class Script(int seed)
{
    public const int Keys = 3000;
    public static readonly string?[] Names = [null, "", "A", "B", "AB", "BA", "ABA", "ABAB", "AAAA", "0", "10", "BA10", .. Strings(42, "AB01", 48, 8)];
    public static readonly string?[] Codes = [null, "X", .. Strings(43, "XY9", 28, 4)];
    public static readonly string?[] Countries = [null, "PL", "US", "DE", "GB"];

    private readonly Random _rnd = new(seed);
    private Item? _reinsert;

    public Dictionary<string, Item> Model { get; } = [];

    public C Next()
    {
        if (_reinsert is { } again)
        {
            _reinsert = null;
            return Upsert(again);
        }
        var r = _rnd.NextDouble();
        if (Model.Count == 0 || r < 0.30) return Upsert(NewItem(RandomKey()));  // an insert, or every field replaced
        var existing = RandomExisting();
        if (r < 0.55) return Upsert(ChangeOneField(existing));
        if (r < 0.62) return Upsert(existing with { });                         // same values, new instance
        if (r < 0.85) return Delete(existing.Key);
        if (r < 0.92) return Delete(RandomKey());                               // usually a miss, may hit
        _reinsert = NewItem(existing.Key);
        return Delete(existing.Key);
    }

    private C Upsert(Item item)
    {
        Model[item.Key] = item;
        return new C.Upsert(item.Key, item);
    }

    private C Delete(string key)
    {
        Model.Remove(key);
        return new C.Delete(key);
    }

    private string RandomKey() => "K" + _rnd.Next(Keys);

    private Item RandomExisting()
    {
        while (true)
            if (Model.TryGetValue(RandomKey(), out var item)) return item;
    }

    private Item NewItem(string key) => new(key, Pick(Names), Pick(Codes), (Kind)_rnd.Next(4), Pick(Countries));

    private Item ChangeOneField(Item x) => _rnd.Next(4) switch
    {
        0 => x with { Name = Pick(Names) },
        1 => x with { Code = Pick(Codes) },
        2 => x with { Kind = (Kind)_rnd.Next(4) },
        _ => x with { Country = Pick(Countries) },
    };

    private string? Pick(string?[] pool) => pool[_rnd.Next(pool.Length)];

    private static IEnumerable<string> Strings(int seed, string alphabet, int count, int maxLength)
    {
        var rnd = new Random(seed);
        for (var i = 0; i < count; i++) yield return new string(Enumerable.Range(0, rnd.Next(1, maxLength + 1)).Select(_ => alphabet[rnd.Next(alphabet.Length)]).ToArray());
    }
}

public static class Checks
{
    // Every substring of every pool value up to 3 chars, every whole value, "", and misses (unknown char, longer than any value).
    private static readonly string[] NameQueries = Queries(Script.Names);
    private static readonly string[] CodeQueries = Queries(Script.Codes);

    // The cache answers every query exactly as the model does.
    public static void Matches(Fixture f, Dictionary<string, Item> model)
    {
        Assert.Equal(CacheStatus.Ready, f.Cache.Status);
        // The counts the entities and index.entries gauges report.
        Assert.Equal(model.Count, f.Cache.Count);
        Assert.Equal(model.Values.Count(x => x.Name is not null), f.ByName.Count);
        Assert.Equal(model.Values.Count(x => x.Name is not null), f.ByNameLucene.Count);
        Assert.Equal(model.Values.Count(x => x.Code is not null), f.ByCode.Count);
        Assert.Equal(model.Count, f.ByKind.Count);
        Assert.Equal(model.Values.Count(x => x.Country is not null), f.ByCountry.Count);
        for (var k = 0; k < Script.Keys; k++) f.Current(model.GetValueOrDefault("K" + k), f.Cache.Get("K" + k));
        foreach (var kind in Enum.GetValues<Kind>()) Finds(f, model, x => x.Kind == kind, () => f.ByKind.Get(kind));
        foreach (var country in Script.Countries.OfType<string>().Append("XX")) Finds(f, model, x => x.Country == country, () => f.ByCountry.Get(country));
        foreach (var q in NameQueries)
        {
            Text(f, model, x => x.Name, q, f.ByName);
            Text(f, model, x => x.Name, q, f.ByNameLucene);
        }
        foreach (var q in CodeQueries) Text(f, model, x => x.Code, q, f.ByCode);
    }

    public static void Hydrating(Fixture f)
    {
        Assert.Equal(CacheStatus.Hydrating, f.Cache.Status);
        Assert.Throws<InvalidOperationException>(() => f.Cache.Get("K0"));
        Assert.Throws<InvalidOperationException>(() => f.ByName.StartsWith("A"));
        Assert.Throws<InvalidOperationException>(() => f.ByNameLucene.EndsWith("A"));
        Assert.Throws<InvalidOperationException>(() => f.ByCode.Contains("X"));
        Assert.Throws<InvalidOperationException>(() => f.ByKind.Get(Kind.Bond));
        Assert.Throws<InvalidOperationException>(() => f.ByCountry.Get("PL"));
    }

    private static void Text(Fixture f, Dictionary<string, Item> model, Func<Item, string?> keyOf, string q, TextIndex<Item> index)
    {
        Finds(f, model, x => keyOf(x)?.StartsWith(q, StringComparison.Ordinal) == true, () => index.StartsWith(q));
        Finds(f, model, x => keyOf(x)?.EndsWith(q, StringComparison.Ordinal) == true, () => index.EndsWith(q));
        // LuceneIndex finds nothing for Contains(""): "" is no n-gram term.
        if (q.Length > 0 || index is not LuceneTextIndex<Item>)
            Finds(f, model, x => keyOf(x)?.Contains(q, StringComparison.Ordinal) == true, () => index.Contains(q));
    }

    // The query finds exactly the model's matching entities, each once and as its current version; Count is right before
    // anything is read; and the result reads the same enumerated, by index front to back, and back to front
    // (SortedArrayIndex's Contains finds its hits only as they are read).
    private static void Finds(Fixture f, Dictionary<string, Item> model, Func<Item, bool> matches, Func<IReadOnlyList<Item>> query)
    {
        var expected = model.Values.Where(matches).OrderBy(x => x.Key, StringComparer.Ordinal).ToList();
        var result = query();
        Assert.Equal(expected.Count, result.Count);
        var enumerated = result.ToList();
        var actual = enumerated.OrderBy(x => x.Key, StringComparer.Ordinal).ToList();
        Assert.Equal(expected.Select(x => x.Key), actual.Select(x => x.Key));
        for (var i = 0; i < expected.Count; i++) f.Current(expected[i], actual[i]);

        var forward = query();
        Assert.Equal(enumerated, Enumerable.Range(0, forward.Count).Select(i => forward[i]));
        var backward = query();
        Assert.Equal(Enumerable.Reverse(enumerated), Enumerable.Range(0, backward.Count).Reverse().Select(i => backward[i]));
    }

    private static string[] Queries(string?[] pool)
    {
        var values = pool.OfType<string>().ToArray();
        var substrings = from v in values from start in Enumerable.Range(0, v.Length) from length in Enumerable.Range(1, Math.Min(3, v.Length - start)) select v.Substring(start, length);
        return [.. substrings.Concat(values).Append("").Append("Z").Append(values.MaxBy(x => x.Length) + "A").Distinct()];
    }
}
