# clusterpath

`clusterpath` is a bounded-memory Go library and CLI for reducing URL
cardinality before indexing request streams in ClickHouse or another analytics
store. It learns structural path and query patterns online, then emits stable
templates.

Requires Go 1.21 or newer. Licensed under the [MIT License](LICENSE).

## Before and after

These examples are taken from the tracked
[`testdata/url-clusters.txt`](testdata/url-clusters.txt) fixture. The first
normalizes a host, drops tracking/session data, sorts retained query keys, and
masks an ID. The other two show numeric and hexadecimal path masking.

| Input | Template |
|---|---|
| `HTTPS://www.example.test/products/electronics/10293?session=abc123_456&category=tvs&sort=price_asc#reviews` | `https://example.test/products/electronics/{id}?category=tvs&sort=price_asc` |
| `/api/tag/entity_property_values/App%5CEntity%5CEdu%5CFormation/100001` | `/api/tag/entity_property_values/App%5CEntity%5CEdu%5CFormation/{id}` |
| `/resultat/candidat/0000000100000065000000c90000012d.html` | `/resultat/candidat/{hex}.html` |

## Properties

- One streaming pass for learning, with optional `Freeze` for stable replay.
- Fixed-size parsing scratch, cardinality sketches, and heavy-hitter tracking.
- Preallocated slab LRU with an open-addressed `uint64` index.
- Zero allocations in `Normalize` and `Apply` when the destination has capacity.
- Stateless masking for numeric IDs, long hexadecimal IDs, UUIDs, random tokens, and build fingerprints.
- Learned masking for high-cardinality literal path positions.
- Query-key sorting and typed high-cardinality value templates.
- Shared-nothing worker model through structural sharding.

The public placeholder vocabulary is `{id}`, `{hex}`, `{uuid}`, `{token}`,
`{slug}`, `{value}`, `{hash}`, and `{more}`. Treat it as part of the output
schema when storing templates downstream.

## Library

```go
c := clusterpath.New(clusterpath.DefaultConfig())
dst := make([]byte, 0, 512)

for _, rawURL := range input {
    dst = c.Normalize(dst[:0], rawURL) // learns and renders
    emit(dst)
}

c.Freeze()
dst = c.Apply(dst[:0], rawURL) // stable replay, no learning or LRU changes
```

A `Clusterer` belongs to one goroutine, including after `Freeze`. For parallel
ETL, create a `Sharded`, use `Shard(rawURL)` to route each URL, and give each
`At(index)` result to exactly one worker. `Shard` itself is safe for concurrent
use.

## CLI

Run the default two-pass mode against the included fixture. The first pass
learns, the second emits stable templates, and `-report` writes a frequency
census:

```sh
go run ./cmd/clusterpath \
  -in testdata/url-clusters.txt \
  -out normalized.paths \
  -report clusters.tsv
```

The current fixture has 1,000 URLs. With the default configuration, the report
ends with:

```text
# paths=1000 clusters=30 buckets=10 evictions=0
```

For a non-seekable stream, use `-two-pass=false`. That mode learns and renders
in one pass, so earlier output can differ from later output as the model learns.

## Query parameters

By default, `utm_*`, `gclid`, `dclid`, `fbclid`, `msclkid`, `_ga`, `session*`,
`token`, and `auth_token` are removed. `session*` and token keys are excluded
to avoid putting secret or per-user values into templates. Use `DropParams` to
change this list, pass a non-nil empty slice to disable it, and use `KeepParams`
to retain specific keys even when they would otherwise be dropped or templated.

## Performance

`Normalize` and `Apply` follow Go's append convention. Calls allocate nothing
when `dst` has sufficient capacity; growing the caller-provided destination
naturally allocates. Verify on the target machine and representative workload:

```sh
go test ./...
go test -run '^$' -bench . -benchmem
```

The default memory budget is 4,096 structural buckets. A bucket uses about
6 KB, plus the index. If the active set of shapes exceeds `MaxBuckets`, the LRU
will evict buckets and learned reduction quality drops. Monitor `Stats()` and
size the cache above the expected simultaneous shape count per worker.

## Budgeted metric clusters

`MetricClusterer` provides a tenant-scoped URL dimension with a hard upper
bound, suitable for aggregating request and cache-miss counters. It is separate
from `Clusterer`: URL normalization can learn precise templates while metrics
emit a fixed, dashboard-safe number of cluster IDs.

The default budget is 96 IDs per tenant/site:

| IDs | Purpose |
|---:|---|
| 8 | Fixed `api`, `image`, `script`, `style`, `font`, `page`, `media`, and `other` fallbacks |
| 58 | Highest-impact canonical route templates |
| 30 | Deterministic host, first-path-segment, and resource-class families |

Route and family selection weights request share and absolute cache-miss share
equally. Active IDs receive a promotion advantage so a new route must be
meaningfully more important before it displaces an existing cluster. Query
strings are never included in metric labels.

Train and freeze the normalizer before recording metric observations. With an
immutable template model, `Rebalance` is the only operation that changes
assignments. Call it at a reporting-window boundary, persist the resulting
dictionary with its effective time, and then call `ResetWindow`. IDs can be
reused after a future rebalance, so historical dashboard queries must resolve
an ID with the dictionary active at the data timestamp.

### Metric CLI report

```sh
go run ./cmd/metriccluster \
  -in testdata/url-clusters.txt \
  -out metric_clusters.tsv
```

The command reads its input three times: it trains normalization, selects a
metric dictionary with the frozen model, then assigns every URL and writes
aggregate counts. Input must be a seekable regular file.

### Online windowed CLI report

For a single-pass stream, pass `-window-size`. The command learns templates
online and rebalances the 96-ID dictionary after each window. Its report keeps
each dictionary version separate, since the same cluster ID may represent a
different route after a later rebalance:

```sh
cat requests.tsv | go run ./cmd/metriccluster \
  -in - \
  -window-size 100000 \
  -cache-miss-column 1 \
  -out windowed_metric_clusters.tsv
```

The output has one row for every active ID in every dictionary version, with `hits`
and optional cache-miss totals. The initial window uses only fallback IDs while
the command gathers candidates; subsequent windows use the dictionary selected
from the preceding window. This mode is appropriate when each version is
stored with its reporting window. Use a persisted frozen dictionary for stable
IDs across all windows.

For tab-separated input containing a cache-miss flag (`0`/`1`, `hit`/`miss`,
or `false`/`true`), pass its zero-based column number:

```sh
go run ./cmd/metriccluster \
  -in requests.tsv \
  -cache-miss-column 1 \
  -out metric_clusters.tsv
```

### Library usage

Train from representative history before serving live events so the initial
dictionary is useful rather than fallback-heavy:

```go
m := clusterpath.NewMetricClusterer(clusterpath.MetricConfig{MaxClusters: 96})

for _, event := range historicalEvents {
    m.Train([]byte(event.URL))
}
m.FreezeNormalizer()
for _, event := range historicalEvents {
    m.Observe([]byte(event.URL), event.CacheMiss)
}
m.Rebalance()
dictionaryStore.Save(tenantID, time.Now(), m.Clusters())
m.ResetWindow()
```

For every live event, write additive counters keyed by tenant, time bucket, and
the returned ID:

```go
cluster := m.Observe([]byte(event.URL), event.CacheMiss)
misses := uint64(0)
if event.CacheMiss {
    misses = 1
}
metricStore.Add(event.Timestamp, tenantID, cluster.ID, 1, misses)
```

Flush the current window before changing its dictionary:

```go
metricStore.Flush()
m.Rebalance()
dictionaryStore.Save(tenantID, time.Now(), m.Clusters())
m.ResetWindow()
```

Use `Assign` to replay URLs against the current dictionary without changing
selection counters. Calculate cache-miss rate from aggregate counters, never
from an average of per-request rates: `sum(cache_misses) / sum(requests)`.
`MetricClusterer` belongs to one goroutine; run one per tenant or serialize
access to a shared tenant dictionary.

## Tuning cardinality reduction

Three knobs control how aggressively templates collapse. Fewer clusters is not
automatically better: over-masking destroys useful taxonomy (a section name
turned into `{slug}`). The goal is to mask near-unique leaves while keeping
bounded enums literal.

- **`SignaturePrefix` / `GroupByShape`**: the highest-impact lever. By default
  the first path segment is folded into the bucket key. Setting
  `SignaturePrefix: clusterpath.GroupByShape` buckets by structural shape only,
  pooling samples across sections. Keep `HighCardRatio` high to avoid masking
  category segments shared by unrelated families.
- **`MinSamples`**: lower it (for example, 8) so small families collapse sooner.
- **`HighCardRatio`**: raise it (for example, 0.8) so only near-unique positions
  are masked. Bounded enums have a low distinct/total ratio and stay literal.
- **`DistinctLimit`**: masks a position after an absolute estimated cardinality.
  Values above 1,420 are clamped because of the fixed-size cardinality sketch.

On the included fixture, the defaults yield 30 templates. This more aggressive
configuration yields 22 while retaining the tested API endpoint taxonomy:

```sh
go run ./cmd/clusterpath -in testdata/url-clusters.txt -report clusters.tsv \
  -signature-prefix -1 -min-samples 8 -distinct-limit 48 -high-card-ratio 0.8
```
