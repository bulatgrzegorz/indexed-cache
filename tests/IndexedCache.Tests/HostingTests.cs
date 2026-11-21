using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using C = IndexedCache.Change<string, IndexedCache.Tests.Item>;

namespace IndexedCache.Tests;

public class HostingTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task HostFeedsTheCacheAndTheIndexesConsumersInject()
    {
        var channel = Channel.CreateUnbounded<C>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddCache<string, Item>()
            .FromSource(_ => new ChannelSource<string, Item>(channel.Reader))
            .AddIndex<ByName>()
            .AddIndex<ByNameLucene>()
            .AddIndex<ByCode>()
            .AddIndex<ByKind>()
            .AddIndex<ByCountry>();
        // Disposing it (Host.Dispose runs DisposeAsync) disposes every index through both its registrations: must not throw (LuceneIndex deletes its folder).
        using var host = builder.Build();
        await host.StartAsync();
        var services = host.Services;
        var cache = services.GetRequiredService<Cache<string, Item>>();

        Assert.Equal(HealthStatus.Unhealthy, await Health(services, "ready"));
        Assert.Equal(HealthStatus.Healthy, await Health(services, "live"));

        var apple = new Item("K1", "APPLE", "X1", Kind.Stock, "US");
        await channel.Writer.WriteAsync(new C.Upsert("K1", apple));
        await channel.Writer.WriteAsync(new C.CaughtUp());
        
        await cache.WaitUntilReadyAsync().WaitAsync(Timeout);
        Assert.Equal(HealthStatus.Healthy, await Health(services, "ready"));
        
        Assert.Same(apple, cache.Get("K1"));
        Assert.Same(apple, Assert.Single(services.GetRequiredService<ByName>().StartsWith("APP")));
        Assert.Same(apple, Assert.Single(services.GetRequiredService<ByNameLucene>().Contains("PPL")));
        Assert.Same(apple, Assert.Single(services.GetRequiredService<ByCode>().EndsWith("1")));
        Assert.Same(apple, Assert.Single(services.GetRequiredService<ByKind>().Get(Kind.Stock)));
        Assert.Same(apple, Assert.Single(services.GetRequiredService<ByCountry>().Get("US")));

        var banana = apple with { Name = "BANANA", Country = "PL" };
        await channel.Writer.WriteAsync(new C.Upsert("K1", banana));
        // One writer, in order: once K2 is in, K1's change has reached every index.
        await channel.Writer.WriteAsync(new C.Upsert("K2", new Item("K2", null, null, Kind.Fund, null)));
        await Until(() => cache.Get("K2") is not null);

        Assert.Empty(services.GetRequiredService<ByName>().StartsWith("APP"));
        Assert.Same(banana, Assert.Single(services.GetRequiredService<ByName>().StartsWith("BAN")));
        Assert.Same(banana, Assert.Single(services.GetRequiredService<ByNameLucene>().Contains("NAN")));
        Assert.Empty(services.GetRequiredService<ByCountry>().Get("US"));
        Assert.Same(banana, Assert.Single(services.GetRequiredService<ByCountry>().Get("PL")));

        await host.StopAsync();
    }

    [Fact]
    public async Task UseRocksDbKeepsTheEntitiesInRocksDbUntilTheHostIsDisposed()
    {
        var directory = Directory.CreateTempSubdirectory("indexed-cache-test-").FullName;
        var channel = Channel.CreateUnbounded<C>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddCache<string, Item>().FromSource(_ => new ChannelSource<string, Item>(channel.Reader)).UseRocksDb(directory).AddIndex<ByName>();
        using (var host = builder.Build())
        {
            await host.StartAsync();
            var cache = host.Services.GetRequiredService<Cache<string, Item>>();
            var apple = new Item("K1", "APPLE", "X1", Kind.Stock, "US");
            await channel.Writer.WriteAsync(new C.Upsert("K1", apple));
            await channel.Writer.WriteAsync(new C.CaughtUp());
            await cache.WaitUntilReadyAsync().WaitAsync(Timeout);

            // Equal, not the same instance: read back from RocksDB.
            Assert.Equal(apple, cache.Get("K1"));
            Assert.NotSame(apple, cache.Get("K1"));
            Assert.Equal(apple, Assert.Single(host.Services.GetRequiredService<ByName>().StartsWith("APP")));
            Assert.Single(Directory.GetDirectories(directory));
            await host.StopAsync();
        }
        Assert.Empty(Directory.EnumerateFileSystemEntries(directory));
        Directory.Delete(directory);
    }

    [Fact]
    public async Task FailedSourceMakesLiveUnhealthy()
    {
        var channel = Channel.CreateUnbounded<C>();
        var builder = Host.CreateApplicationBuilder();
        // Default (StopHost) would stop the host; here it keeps running so the check can be asked.
        builder.Services.Configure<HostOptions>(o => o.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.Ignore);
        builder.Services.AddCache<string, Item>().FromSource(_ => new ChannelSource<string, Item>(channel.Reader)).AddIndex<ByKind>();
        using var host = builder.Build();
        await host.StartAsync();
        var cache = host.Services.GetRequiredService<Cache<string, Item>>();

        await channel.Writer.WriteAsync(new C.CaughtUp());
        await cache.WaitUntilReadyAsync().WaitAsync(Timeout);
        channel.Writer.Complete(new IOException("broker gone"));
        await Until(() => cache.Status == CacheStatus.Faulted);

        Assert.Equal(HealthStatus.Unhealthy, await Health(host.Services, "live"));
        await host.StopAsync();
    }

    private static async Task<HealthStatus> Health(IServiceProvider services, string tag) =>
        (await services.GetRequiredService<HealthCheckService>().CheckHealthAsync(r => r.Tags.Contains(tag))).Status;

    private static Task Until(Func<bool> condition) => Task.Run(async () =>
    {
        while (!condition()) await Task.Delay(10);
    }).WaitAsync(Timeout);
}
