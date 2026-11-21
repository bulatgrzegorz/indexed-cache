using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace IndexedCache;

public static class CacheServiceCollectionExtensions
{
    // Registers the cache (a singleton), the hosted service that feeds it from its source, and two health checks:
    // cache:<TEntity>:ready (tag "ready", Healthy once hydrated) and cache:<TEntity>:live (tag "live", Unhealthy once Faulted).
    public static CacheBuilder<TKey, TEntity> AddCache<TKey, TEntity>(this IServiceCollection services) where TKey : notnull where TEntity : class
    {
        services.AddSingleton<Cache<TKey, TEntity>>();
        services.AddHostedService<CacheService<TKey, TEntity>>();
        services.AddHealthChecks()
            .Add(Check<TKey, TEntity>("ready", status => status == CacheStatus.Ready))
            .Add(Check<TKey, TEntity>("live", status => status != CacheStatus.Faulted));
        return new CacheBuilder<TKey, TEntity>(services);
    }

    private static HealthCheckRegistration Check<TKey, TEntity>(string tag, Func<CacheStatus, bool> healthy) where TKey : notnull where TEntity : class =>
        new($"cache:{typeof(TEntity).Name}:{tag}", sp => new StatusCheck<TKey, TEntity>(sp.GetRequiredService<Cache<TKey, TEntity>>(), healthy),
            failureStatus: HealthStatus.Unhealthy, tags: [tag]);

    private sealed class StatusCheck<TKey, TEntity>(Cache<TKey, TEntity> cache, Func<CacheStatus, bool> healthy) : IHealthCheck
        where TKey : notnull where TEntity : class
    {
        public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            var status = cache.Status;
            return Task.FromResult(new HealthCheckResult(healthy(status) ? HealthStatus.Healthy : context.Registration.FailureStatus, $"Cache is {status}."));
        }
    }
}

public sealed class CacheBuilder<TKey, TEntity>(IServiceCollection services) where TKey : notnull where TEntity : class
{
    public IServiceCollection Services => services;

    public CacheBuilder<TKey, TEntity> FromSource(Func<IServiceProvider, ICacheSource<TKey, TEntity>> source)
    {
        services.AddSingleton(source);
        return this;
    }

    // Where the entities live: InMemoryStore unless this is called (UseRocksDb from IndexedCache.RocksDb, or your own IEntityStore).
    public CacheBuilder<TKey, TEntity> UseStore(Func<IServiceProvider, IEntityStore<TEntity>> store)
    {
        services.AddSingleton(store);
        return this;
    }

    // TIndex is a singleton consumers inject to query; the cache gets it as one of its ICacheIndex<TEntity>.
    public CacheBuilder<TKey, TEntity> AddIndex<TIndex>() where TIndex : class, ICacheIndex<TEntity>
    {
        services.AddSingleton<TIndex>();
        services.AddSingleton<ICacheIndex<TEntity>>(sp => sp.GetRequiredService<TIndex>());
        return this;
    }
}

internal sealed class CacheService<TKey, TEntity>(Cache<TKey, TEntity> cache, ICacheSource<TKey, TEntity> source) : BackgroundService
    where TKey : notnull where TEntity : class
{
    // Task.Run: a source may block its thread (Kafka's Consume), which must not be the host's.
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(() => cache.RunAsync(source, stoppingToken), stoppingToken);
}
