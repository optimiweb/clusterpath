// Package clusterpath performs bounded-memory, online clustering of URLs into
// normalized templates. It is designed to reduce the cardinality of a
// high-volume URL stream before the data is indexed in an analytics store such
// as ClickHouse, where unbounded distinct path/query values are expensive.
//
// Given a raw URL, [Clusterer.Normalize] returns a template in which
// high-cardinality, low-information components are replaced by placeholders
// while stable structure is preserved:
//
//	/api/tag/entity_property_values/App%5CEntity%5CEdu%5CFormation/261    ->  /api/tag/entity_property_values/App%5CEntity%5CEdu%5CFormation/{id}
//	/resultat/candidat/ca508a0b52086307ea926f194c702566.html             ->  /resultat/candidat/{hex}.html
//	/etudes/annuaire-enseignement-superieur/formation/bts-info-9398.html ->  /etudes/annuaire-enseignement-superieur/formation/bts-info-{id}.html
//	https://www.example.test/p/12?session=abc&sort=asc#top              ->  https://example.test/p/{id}?sort=asc
//
// # How it works
//
// Each URL is parsed and every path segment is classified as one of a small set
// of token classes (numeric id, long hexadecimal id, UUID, random token, or
// literal). Segments that are obviously machine-generated, including build
// fingerprints embedded in static asset names, are masked
// immediately (statelessly). The remaining literal segments and query values
// are handled online:
//
//   - A structural signature (host, depth, per-position class, extension, and an
//     optional literal prefix) maps each URL to a bucket in a fixed-size LRU.
//   - Each bucket estimates the distinct-value cardinality and tracks heavy
//     hitters per path position and per query key using compact sketches.
//   - Once a bucket has seen enough samples, positions whose values are
//     near-unique are rendered as placeholders ({slug}, {id}, ...), while
//     bounded enumerations (sections, categories, API resources) stay literal.
//   - Query keys are sorted; tracking parameters are dropped while
//     high-cardinality values become typed placeholders.
//
// Grouping by structural signature makes clustering O(N) in the number of URLs
// rather than O(N^2): each URL is examined once and routed to its bucket.
//
// # Basic usage
//
//	c := clusterpath.New(clusterpath.DefaultConfig())
//	dst := make([]byte, 0, 512)
//	for _, raw := range stream {
//	    dst = c.Normalize(dst[:0], raw) // learns and renders
//	    emit(dst)
//	}
//
// [Clusterer.Normalize] both learns from and renders each URL. For a stable
// second pass (for example, replaying a backfill so identical inputs always
// map to identical templates), call [Clusterer.Freeze] and then
// [Clusterer.Apply], which renders without mutating any state.
//
// # Memory
//
// All hot-path memory is preallocated by [New]; resident memory is bounded by
// [Config.MaxBuckets] regardless of stream size. Normalize and Apply follow
// Go's append convention and perform no allocations when the destination slice
// has spare capacity.
//
// # Concurrency
//
// A [Clusterer] is not safe for concurrent use. For parallel ingestion, use
// [Sharded]: it owns one independent Clusterer per worker. Route each URL with
// [Sharded.Shard] (safe for concurrent use) and process it on the single
// goroutine that owns [Sharded.At](shard). Because a Clusterer is
// allocation-free and lock-free, throughput scales linearly with cores.
//
// # Tuning
//
// The defaults from [DefaultConfig] favor precision. To collapse the long tail
// more aggressively while preserving taxonomy, set
// [Config.SignaturePrefix] to [GroupByShape], lower [Config.MinSamples], and
// raise [Config.HighCardRatio]. See the type documentation for the trade-offs.
package clusterpath
