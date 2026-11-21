# indexed-cache

A cache of whole entities by key, fed from a source (a compacted Kafka topic, or anything that implements `ICacheSource`).
The entities are kept in memory, in RocksDB, or in any `IEntityStore` you write. It keeps every index you declare up to date. The text index is `SortedArrayIndex` from
[search-algorithms](https://github.com/bulatgrzegorz/search-algorithms), copied into `src/IndexedCache/Text/` with its names unchanged, so fixes port over as plain diffs.
The copy adds two guards for an empty build (no items, or only `""` keys), which the cache does when it hydrates nothing.

```csharp
public sealed class InstrumentsByName : TextIndex<Instrument>            // StartsWith / EndsWith / Contains, SortedArrayIndex
{
    protected override string? KeyOf(Instrument x) => x.Name;
}
public sealed class InstrumentsByType : ExactIndex<Instrument, InstrumentType>
{
    protected override InstrumentType ValueOf(Instrument x) => x.Type;
}
// LuceneTextIndex<T> (IndexedCache.Lucene) is the same API on Lucene.Net.

services.AddCache<string, Instrument>()
    .FromKafka(new ConsumerConfig { BootstrapServers = "..." }, "instruments", Deserializers.Utf8, instrumentDeserializer)
    .AddIndex<InstrumentsByName>()
    .AddIndex<InstrumentsByType>();

// inject Cache<string, Instrument>, InstrumentsByName, InstrumentsByType
cache.Get("US0378331005");
byName.StartsWith("APP");          // IReadOnlyList<Instrument>: Count is cheap, entities are looked up as they are read
byType.Get(InstrumentType.Bond);
```

## Lifecycle

- **Hydrating.** The source replays everything that exists (Kafka reads every partition from offset 0, with no consumer group). Changes go into the store only, and every query throws.
- **CaughtUp.** Every index is built once from the whole store. A bulk build is far cheaper than an `Add` per entity: 1M ISINs hydrate in about 2 s, while 100k upserts into indexes that are already live take about 1.7 s.
- **Ready.** Each change updates the store and then every index. Health checks: `cache:<Entity>:ready` (tag `ready`) and `cache:<Entity>:live` (tag `live`, Unhealthy once the source has failed).
- **Faulted.** A source error is rethrown, never skipped. The host stops (the BackgroundService default), and the next start rehydrates. The data already loaded can still be read.

## Consistency

Each change updates the store, then the indexes one at a time, from a single writer, while reads run concurrently. Every result is a consistent snapshot of its own index. Across indexes, consistency is eventual:

- A hit may resolve to a newer version of its entity than the one that matched.
- During a text-index update, the entity is removed before it is re-added, so for a moment a query may miss it. It never shows up twice.
- A deleted entity's slot keeps its last version until restart. A result that is still being read therefore never gets a null.

## Storage

The cache gives every key an int id, and the indexes hold those ids. The key → id map always stays in memory. The entities, by id, go to an `IEntityStore<TEntity>`:

```csharp
services.AddCache<string, Instrument>()
    .UseRocksDb()                          // IndexedCache.RocksDb; leave it out for InMemoryStore
    // .UseStore(sp => new MyStore(...))   // or any IEntityStore<Instrument>
    ...
```

- **`InMemoryStore`** (the default) is an array of references. A read is an array access, and a result returns the very instance the source delivered.
- **`RocksDbStore`** keeps the entities off the managed heap. It serializes them with System.Text.Json unless you pass `serialize` / `deserialize`. Every read is a RocksDB `Get` plus a deserialize, and each read returns a new instance. That covers:
  - `Cache.Get`;
  - each result item, as it is read;
  - the old version, on every live change;
  - every entity once per index, during the build.

  Since every start rehydrates, the database lives in a new folder under `directory` (the temp folder by default). It has no WAL and is deleted when the container is disposed.
- **Your own store** implements four members: `this[id]`, `All`, `Set` and `Remove`. The contract is in `Entities.cs`:
  - the cache hands out ids in order and never reuses one;
  - `Set` and `Remove` come from a single writer;
  - `this[id]` must still return the last version after `Remove`;
  - a version must be readable as soon as `Set` returns.

`ConsistencyTests` runs every scenario against both stores.

## Telemetry

The cache uses the built-in .NET APIs: `System.Diagnostics.Metrics`, `ActivitySource` and `ILogger`. It needs no OpenTelemetry package, so wiring it up is two lines:

```csharp
services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter(CacheTelemetry.Name))   // "IndexedCache"
    .WithTracing(t => t.AddSource(CacheTelemetry.Name));
```

**Metrics.** Every metric is tagged `cache.name`, the entity type's name. Durations are in seconds, with bucket advice from 1 µs to 60 s (OpenTelemetry's default buckets are meant for milliseconds).

| Instrument | Type | Extra tags | Answers |
|---|---|---|---|
| `indexed_cache.status` | gauge | `cache.status` (`hydrating`, `ready`, `faulted`; 1 = current) | Is it ready? Has it faulted? |
| `indexed_cache.entities` | gauge | | How many entities are in the cache? |
| `indexed_cache.index.entries` | gauge | `cache.index` | How many entities does each index hold? Compare with `entities` to spot an index losing data. |
| `indexed_cache.last_change.age` | gauge (s) | | Is the source still feeding the cache? |
| `indexed_cache.change.duration` | histogram | `cache.change` (`insert`, `update`, `delete`, `delete_missing`), `cache.phase` (`hydrating`, `live`) | The rate and cost of each kind of change. |
| `indexed_cache.index.update.duration` | histogram | `cache.index` | Which index makes writes slow? |
| `indexed_cache.index.build.duration` | histogram | `cache.index` | Which index makes startup slow? |
| `indexed_cache.hydration.duration` | histogram | | How long a start takes until the cache is ready. |
| `indexed_cache.query.duration` | histogram | `cache.index`, `cache.query` (`starts_with`, `ends_with`, `contains`, `get`) | Query rate and latency per index. Results are lazy, so the time spent reading them is not included. |
| `indexed_cache.lookups` | counter | `cache.found` | The hit ratio of `Get(key)`. |
| `indexed_cache.kafka.lag` | gauge | `messaging.destination.name`, `messaging.destination.partition.id` | How far behind the topic each partition is. While hydrating, this is how much is left. Offsets are counted, transaction markers included, as in Kafka's own consumer lag. |

**Traces.**
- An `indexed_cache.hydrate` span runs from start until the cache is ready. It carries the `cache.entities` and `cache.changes` tags. If hydration fails, the span has Error status and an exception event.
- Each index build gets a child span, `indexed_cache.build_index`.
- Individual changes and queries get no spans: there can be millions of them, so the histograms cover them instead.

**Logs.**
- `ILogger<Cache<,>>` records each lifecycle step: hydrating, each index built (entity count and time), ready (entity count, change count and time), faulted (as an error, with the exception), stopped, and source ended.
- `ILogger<KafkaSource<,>>` records the start (partition count and how much is left to hydrate), the catch-up point, and the Kafka client's own logs and errors. Without it, an unreachable broker only shows up as a cache stuck in `hydrating`.

## Tests

`dotnet test`. The Kafka test needs Docker or Podman (Testcontainers) and is skipped without it.

`ConsistencyTests` makes 30k random changes over small, overlapping value pools: shared values, values that are prefixes or substrings of others, `""`, null, deletes and re-inserts. At every 1k changes it checks every index against a reference dictionary:
- every query, run exhaustively;
- each result checked for exact contents, no duplicates, current instances, and the same reads whether enumerated, read front to back or read back to front.

It also runs concurrent readers during writes, and a series of failure cases.
