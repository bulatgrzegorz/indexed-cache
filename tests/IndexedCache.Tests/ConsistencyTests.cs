using C = IndexedCache.Change<string, IndexedCache.Tests.Item>;

namespace IndexedCache.Tests;

// After many random changes, every index still finds exactly what the model holds: nothing lost, nothing stale, nothing twice.
// ~2k live entities and 30k changes, so SortedArrayIndex merges its delta (every 256 pending changes) many times.
public class ConsistencyTests
{
    private const int Operations = 30_000, CheckEvery = 1_000;

    [Theory]
    [InlineData(1, 5_000, Store.InMemory)]      // hydrate 5k changes, then live
    [InlineData(2, 0, Store.InMemory)]          // nothing to hydrate: indexes built empty, everything live
    [InlineData(3, 1, Store.InMemory)]          // built from one entity
    [InlineData(4, 12_345, Store.InMemory)]
    [InlineData(5, Operations, Store.InMemory)] // everything hydrated, nothing live
    [InlineData(1, 5_000, Store.RocksDb)]
    [InlineData(2, 0, Store.RocksDb)]
    [InlineData(3, 1, Store.RocksDb)]
    [InlineData(4, 12_345, Store.RocksDb)]
    [InlineData(5, Operations, Store.RocksDb)]
    public async Task NothingLostAfterManyOperations(int seed, int caughtUpAt, Store store)
    {
        await using var f = new Fixture(store);
        var script = new Script(seed);

        await f.Cache.RunAsync(new ScriptSource(Changes()), CancellationToken.None);

        IEnumerable<C> Changes()
        {
            for (var i = 0; ; i++)
            {
                // Here the cache has applied every change yielded so far.
                if (i == caughtUpAt) yield return new C.CaughtUp();
                if (i % CheckEvery == 0 || i == Operations)
                {
                    if (i >= caughtUpAt) Checks.Matches(f, script.Model);
                    else Checks.Hydrating(f);
                }
                if (i == Operations) yield break;
                yield return script.Next();
            }
        }
    }

    // Readers on 4 threads query every index while 50k changes go in. Each result must be whole: Count matches what it
    // enumerates, no entity twice, no nulls. Afterwards the cache must match the model exactly.
    [Theory]
    [InlineData(Store.InMemory)]
    [InlineData(Store.RocksDb)]
    public async Task ConcurrentReadsDuringWrites(Store store)
    {
        await using var f = new Fixture(store);
        var script = new Script(7);
        using var stop = new CancellationTokenSource();
        using var reading = new CountdownEvent(4);

        var writer = f.Cache.RunAsync(new ScriptSource(Changes()), CancellationToken.None);
        await f.Cache.WaitUntilReadyAsync();
        var readers = Enumerable.Range(0, 4).Select(seed => Task.Run(() => Read(f, seed, reading, stop.Token))).ToArray();
        // The readers stop (and are awaited) even if the writer fails: the fixture must not dispose the store under them.
        var failure = await Record.ExceptionAsync(() => writer);
        await stop.CancelAsync();
        await Task.WhenAll(readers);
        Assert.Null(failure);
        Checks.Matches(f, script.Model);
        return;

        IEnumerable<C> Changes()
        {
            for (var i = 0; i < 2_000; i++) yield return script.Next();
            yield return new C.CaughtUp();
            // The live changes start once every reader has read: on a busy machine the pool may start them late.
            Assert.True(reading.Wait(TimeSpan.FromSeconds(30)));
            for (var i = 0; i < 50_000; i++) yield return script.Next();
        }
    }

    private static void Read(Fixture f, int seed, CountdownEvent reading, CancellationToken stop)
    {
        var rnd = new Random(seed);
        for (var reads = 0; !stop.IsCancellationRequested; reads++)
        {
            if (reads == 1) reading.Signal();
            var name = Script.Names[rnd.Next(1, Script.Names.Length)]!;
            var q = name[..rnd.Next(name.Length + 1)];
            var result = rnd.Next(7) switch
            {
                0 => f.ByName.StartsWith(q),
                1 => f.ByName.EndsWith(q),
                2 => f.ByName.Contains(q),
                3 => f.ByNameLucene.Contains(q),
                4 => f.ByCode.StartsWith(Script.Codes[rnd.Next(1, Script.Codes.Length)]!),
                5 => f.ByKind.Get((Kind)rnd.Next(4)),
                _ => f.ByCountry.Get(Script.Countries[rnd.Next(1, Script.Countries.Length)]!),
            };
            var count = result.Count;
            var items = result.ToList();
            Assert.Equal(count, items.Count);
            Assert.All(items, x => Assert.NotNull(x));
            Assert.Equal(items.Count, items.DistinctBy(x => x.Key).Count());
        }
    }

    [Fact]
    public async Task SourceFailureFaultsButKeepsServing()
    {
        await using var f = new Fixture();
        var apple = new Item("K1", "APPLE", "X", Kind.Stock, "US");

        await Assert.ThrowsAsync<IOException>(() => f.Cache.RunAsync(new ScriptSource(Changes()), CancellationToken.None));

        Assert.Equal(CacheStatus.Faulted, f.Cache.Status);
        Assert.Same(apple, f.Cache.Get("K1"));
        Assert.Same(apple, Assert.Single(f.ByName.StartsWith("AP")));

        IEnumerable<C> Changes()
        {
            yield return new C.Upsert("K1", apple);
            yield return new C.CaughtUp();
            throw new IOException("broker gone");
        }
    }

    [Fact]
    public async Task SourceEndingBeforeCaughtUpFaults()
    {
        await using var f = new Fixture();

        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Cache.RunAsync(new ScriptSource([new C.Upsert("K1", new Item("K1", "A", null, Kind.Bond, null))]), CancellationToken.None));

        Assert.Equal(CacheStatus.Faulted, f.Cache.Status);
        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Cache.WaitUntilReadyAsync());
        Assert.Throws<InvalidOperationException>(() => f.Cache.Get("K1"));
    }

    [Fact]
    public async Task OneWriterOnly()
    {
        await using var f = new Fixture();
        using var stop = new CancellationTokenSource();
        var first = f.Cache.RunAsync(new ScriptSource(Forever()), stop.Token);

        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Cache.RunAsync(new ScriptSource([]), CancellationToken.None));

        await stop.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        Assert.NotEqual(CacheStatus.Faulted, f.Cache.Status);

        static IEnumerable<C> Forever()
        {
            yield return new C.CaughtUp();
            while (true) yield return new C.Delete("K0");
        }
    }
}
