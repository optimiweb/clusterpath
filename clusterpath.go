package clusterpath

const defaultSeed uint64 = 0x243f6a8885a308d3

// GroupByShape is a SignaturePrefix value that disables literal-prefix folding
// so URLs are bucketed purely by structural shape (host, depth, per-position
// class, and extension). This aggregates far more samples per bucket, which
// lets the long tail of low-frequency families cross MinSamples and collapse;
// low-cardinality positions (sections, enums) still render as literals.
const GroupByShape = -1

// Config controls the fixed-memory cache and online cardinality decisions.
// Zero values select defaults. A non-nil empty DropParams disables the default
// denylist.
type Config struct {
	// MaxBuckets bounds resident memory (one structural bucket is about 6 KB).
	MaxBuckets int
	// MinSamples is how many hits a bucket needs before learned masking
	// activates. Lower values collapse rare families sooner.
	MinSamples uint32
	// HighCardRatio is the distinct/total ratio above which a position is
	// treated as variable. Higher values (e.g. 0.8) keep bounded-enum
	// positions (sections, categories) literal and mask only near-unique
	// leaves, which preserves taxonomy while still reducing cardinality.
	HighCardRatio float64
	// DistinctLimit masks a position once its estimated distinct count
	// crosses this absolute bound, regardless of ratio. Keep it above the
	// largest legitimate enum you want to preserve.
	DistinctLimit uint16
	// HeavyHitterMin protects frequent literals in an otherwise high-card
	// position from being masked.
	HeavyHitterMin uint32
	// SignaturePrefix is the number of leading literal path segments folded
	// into the bucket key. The zero value selects the default (1). Set it to
	// GroupByShape to bucket purely by structural shape.
	SignaturePrefix int
	// DropParams lists query keys that are always removed. An entry ending in
	// "*" matches by prefix (for example "utm_*"); matching is
	// case-insensitive. A nil slice selects the default denylist; a non-nil
	// empty slice disables it.
	DropParams []string
	// KeepParams lists query keys that are always retained, overriding both
	// DropParams and learned high-cardinality value templating.
	KeepParams []string
	// Seed randomizes the internal hash functions. The zero value selects a
	// fixed default so output is reproducible across runs; set it to isolate
	// independent instances or to harden against adversarial key collisions.
	Seed uint64
}

// DefaultConfig returns settings suitable for a long-running ETL worker: a
// 4096-bucket cache, conservative masking thresholds that favor precision, and
// a denylist covering common tracking parameters. Pass it to [New], optionally
// overriding individual fields.
func DefaultConfig() Config {
	return Config{
		MaxBuckets:      4096,
		MinSamples:      32,
		HighCardRatio:   0.5,
		DistinctLimit:   64,
		HeavyHitterMin:  4,
		SignaturePrefix: 1,
		DropParams: []string{
			"utm_*", "gclid", "dclid", "fbclid", "msclkid", "_ga",
			"session*", "token", "auth_token",
			"fbbaid", "bbaid", "gad_*", "gbraid", "eff_*", "rtg", "awc",
			"sym_*", "xtatc", "gref", "referrer", "csp", "frz-*",
		},
		Seed: defaultSeed,
	}
}

type matchRule struct {
	value  string
	prefix bool
}

type matcher struct{ rules []matchRule }

func newMatcher(values []string) matcher {
	m := matcher{rules: make([]matchRule, 0, len(values))}
	for _, value := range values {
		if value == "" {
			continue
		}
		rule := matchRule{value: value}
		if value[len(value)-1] == '*' {
			rule.value = value[:len(value)-1]
			rule.prefix = true
		}
		if rule.value == "" {
			// A bare "*" would otherwise match every parameter.
			continue
		}
		m.rules = append(m.rules, rule)
	}
	return m
}

func (m matcher) matches(raw []byte, s span) bool {
	for _, rule := range m.rules {
		if rule.prefix {
			if s.len() >= len(rule.value) && equalFoldString(raw[s.start:s.start+len(rule.value)], rule.value) {
				return true
			}
		} else if s.len() == len(rule.value) && equalFoldString(raw[s.start:s.end], rule.value) {
			return true
		}
	}
	return false
}

func equalFoldString(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i, c := range b {
		other := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if other >= 'A' && other <= 'Z' {
			other += 'a' - 'A'
		}
		if c != other {
			return false
		}
	}
	return true
}

// Clusterer learns URL shapes online in bounded memory. A Clusterer is not safe
// for concurrent use; give each goroutine its own instance (see Sharded).
type Clusterer struct {
	cache           bucketCache
	decisions       decisionConfig
	drop            matcher
	keep            matcher
	seed            uint64
	signaturePrefix int
	frozen          bool
	processed       uint64
	scratch         parsedURL // reused per call to avoid re-zeroing large arrays
}

// New returns a Clusterer that preallocates all hot-path memory up front. Zero-
// valued Config fields are replaced by their [DefaultConfig] equivalents, so
// New(Config{}) is valid and New(Config{MaxBuckets: n}) overrides a single
// setting. A negative SignaturePrefix (see [GroupByShape]) disables literal-
// prefix folding.
func New(cfg Config) *Clusterer {
	defaults := DefaultConfig()
	if cfg.MaxBuckets <= 0 {
		cfg.MaxBuckets = defaults.MaxBuckets
	}
	if cfg.MinSamples == 0 {
		cfg.MinSamples = defaults.MinSamples
	}
	if cfg.HighCardRatio <= 0 || cfg.HighCardRatio > 1 {
		cfg.HighCardRatio = defaults.HighCardRatio
	}
	if cfg.DistinctLimit == 0 {
		cfg.DistinctLimit = defaults.DistinctLimit
	}
	if cfg.HeavyHitterMin == 0 {
		cfg.HeavyHitterMin = defaults.HeavyHitterMin
	}
	if cfg.SignaturePrefix == 0 {
		cfg.SignaturePrefix = defaults.SignaturePrefix
	} else if cfg.SignaturePrefix < 0 {
		cfg.SignaturePrefix = 0
	}
	if cfg.DropParams == nil {
		cfg.DropParams = defaults.DropParams
	}
	if cfg.Seed == 0 {
		cfg.Seed = defaults.Seed
	}
	return &Clusterer{
		cache: newBucketCache(cfg.MaxBuckets),
		decisions: decisionConfig{
			minSamples:     cfg.MinSamples,
			ratioThreshold: uint32(cfg.HighCardRatio*ratioScale + 0.5),
			distinctLimit:  cfg.DistinctLimit,
			heavyMin:       cfg.HeavyHitterMin,
		},
		drop:            newMatcher(cfg.DropParams),
		keep:            newMatcher(cfg.KeepParams),
		seed:            cfg.Seed,
		signaturePrefix: cfg.SignaturePrefix,
	}
}

// Normalize updates the cluster model with raw, then appends raw's normalized
// template to dst and returns the extended slice (like Go's built-in append).
// dst may be nil. The call allocates nothing when dst has sufficient capacity,
// so reusing a scratch buffer across calls keeps the hot path allocation-free:
//
//	dst = c.Normalize(dst[:0], raw)
//
// raw is treated as read-only and is not retained. After [Clusterer.Freeze],
// Normalize behaves like [Clusterer.Apply].
func (c *Clusterer) Normalize(dst, raw []byte) []byte {
	return c.normalize(dst, raw, !c.frozen)
}

// Apply appends raw's normalized template to dst without learning from raw or
// mutating any cache state. Use it for a stable, repeatable pass once the model
// has been trained (typically after [Clusterer.Freeze]). If raw maps to a
// bucket that was never learned, only stateless (class-based) masking applies.
func (c *Clusterer) Apply(dst, raw []byte) []byte {
	return c.normalize(dst, raw, false)
}

func (c *Clusterer) normalize(dst, raw []byte, learn bool) []byte {
	c.processed++
	p := &c.scratch
	parseURL(raw, p)
	analyzeParsed(p, c.seed, true)
	signature := structuralSignature(p, c.seed, c.signaturePrefix)
	b := c.cache.get(signature, learn, learn)
	if learn {
		b.update(p)
	}
	return c.render(dst, p, b)
}

// Freeze stops learning and LRU mutation. Subsequent [Clusterer.Normalize]
// calls behave like [Clusterer.Apply], so identical inputs always produce
// identical templates. Freeze is idempotent.
func (c *Clusterer) Freeze() { c.frozen = true }

// Stats is a snapshot of a Clusterer's counters.
type Stats struct {
	// Processed is the number of Normalize and Apply calls served.
	Processed uint64
	// Buckets is the number of structural buckets currently resident.
	Buckets int
	// Hits is the number of lookups that found an existing bucket.
	Hits uint64
	// Misses is the number of lookups that created (or would have created) a
	// bucket.
	Misses uint64
	// Evictions is the number of buckets discarded because the cache was full.
	// A large value relative to Buckets means MaxBuckets is too small for the
	// working set of shapes and reduction quality will suffer.
	Evictions uint64
}

// Stats returns a snapshot of the current processing and cache counters.
func (c *Clusterer) Stats() Stats {
	return Stats{
		Processed: c.processed,
		Buckets:   c.cache.count,
		Hits:      c.cache.hits,
		Misses:    c.cache.misses,
		Evictions: c.cache.evictions,
	}
}

// Sharded holds a fixed set of independent [Clusterer] instances for
// lock-free parallel processing. A single dispatcher goroutine calls
// [Sharded.Shard] to pick a shard for each URL, then hands the URL to the one
// worker goroutine that owns that shard via [Sharded.At]. Because routing is by
// structural signature, all URLs of the same shape always land on the same
// shard, so per-shard models stay coherent.
type Sharded struct {
	shards          []*Clusterer
	seed            uint64
	signaturePrefix int
}

// NewSharded returns a Sharded with n independent Clusterers, each configured
// from cfg. n is clamped to at least 1. Give each shard to exactly one worker
// goroutine.
func NewSharded(n int, cfg Config) *Sharded {
	if n < 1 {
		n = 1
	}
	if cfg.Seed == 0 {
		cfg.Seed = defaultSeed
	}
	prefix := cfg.SignaturePrefix
	if prefix == 0 {
		prefix = 1
	} else if prefix < 0 {
		prefix = 0
	}
	s := &Sharded{shards: make([]*Clusterer, n), seed: cfg.Seed, signaturePrefix: prefix}
	for i := range s.shards {
		s.shards[i] = New(cfg)
	}
	return s
}

// Shard returns the index, in [0, n), of the shard that owns raw. It depends
// only on raw's structural signature, so identical shapes route consistently.
// Shard is safe for concurrent use.
func (s *Sharded) Shard(raw []byte) int {
	var parsed parsedURL
	parseURL(raw, &parsed)
	analyzeParsed(&parsed, s.seed, false)
	return int(structuralSignature(&parsed, s.seed, s.signaturePrefix) % uint64(len(s.shards)))
}

// At returns the Clusterer for the given shard index, as returned by
// [Sharded.Shard]. The returned Clusterer must be used by a single goroutine.
func (s *Sharded) At(index int) *Clusterer { return s.shards[index] }

// Freeze freezes every shard. See [Clusterer.Freeze].
func (s *Sharded) Freeze() {
	for _, shard := range s.shards {
		shard.Freeze()
	}
}

// Stats returns the per-shard counters summed across all shards.
func (s *Sharded) Stats() Stats {
	var total Stats
	for _, shard := range s.shards {
		stats := shard.Stats()
		total.Processed += stats.Processed
		total.Buckets += stats.Buckets
		total.Hits += stats.Hits
		total.Misses += stats.Misses
		total.Evictions += stats.Evictions
	}
	return total
}
