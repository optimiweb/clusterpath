package clusterpath

import (
	"container/heap"
	"math"
	"sort"
	"strings"
)

const metricFallbackCount = 8

var metricFallbackNames = [metricFallbackCount]string{
	"api", "image", "script", "style", "font", "page", "media", "other",
}

// MetricClusterKind identifies how a metric cluster was selected.
type MetricClusterKind uint8

const (
	MetricFallbackCluster MetricClusterKind = iota
	MetricFamilyCluster
	MetricRouteCluster
)

// MetricCluster is a stable assignment until the next Rebalance call when the
// normalizer has been frozen. Record its ID with the observation time so
// historical aggregates retain their original meaning after a later rebalance.
type MetricCluster struct {
	ID    int
	Key   string
	Label string
	Kind  MetricClusterKind
}

// MetricStats contains additive request and cache-miss counters. Cache-miss
// rates should always be calculated as CacheMisses / Requests after aggregation.
type MetricStats struct {
	Requests    uint64
	CacheMisses uint64
}

// MetricConfig controls a tenant-scoped, budgeted metric cluster dictionary.
// The clusterer is not safe for concurrent use.
type MetricConfig struct {
	// MaxClusters is the hard number of metric IDs, including eight fixed
	// resource-class fallbacks. Zero selects 96.
	MaxClusters int
	// ExactClusters reserves dynamic IDs for canonical route templates. The
	// remaining dynamic IDs represent route families. Zero selects two thirds
	// of the dynamic budget.
	ExactClusters int
	// MaxCandidates bounds route and family statistics retained between window
	// rotations. Zero selects sixteen times MaxClusters, with a floor of 256.
	MaxCandidates int
	// MinSamples is the minimum request count before a route or family can
	// receive a dynamic ID. Zero selects 10.
	MinSamples uint64
	// PromotionRatio gives active IDs a score advantage during Rebalance. A new
	// candidate must be this much stronger to displace one. Zero selects 1.2.
	PromotionRatio float64
	// Normalizer controls canonical URL templating before metric assignment.
	Normalizer Config
}

type metricCandidate struct {
	stats  MetricStats
	label  string
	family string
}

type metricCandidateEntry struct {
	key   string
	stats MetricStats
}

type metricCandidateHeap []metricCandidateEntry

func (h metricCandidateHeap) Len() int { return len(h) }

func (h metricCandidateHeap) Less(i, j int) bool {
	if h[i].stats.Requests != h[j].stats.Requests {
		return h[i].stats.Requests < h[j].stats.Requests
	}
	if h[i].stats.CacheMisses != h[j].stats.CacheMisses {
		return h[i].stats.CacheMisses < h[j].stats.CacheMisses
	}
	return h[i].key > h[j].key
}

func (h metricCandidateHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *metricCandidateHeap) Push(value any) {
	*h = append(*h, value.(metricCandidateEntry))
}

func (h *metricCandidateHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = metricCandidateEntry{}
	*h = old[:last]
	return value
}

type metricCandidateSet struct {
	values map[string]metricCandidate
	heap   metricCandidateHeap
	limit  int
}

func newMetricCandidateSet(limit int) metricCandidateSet {
	return metricCandidateSet{values: make(map[string]metricCandidate, limit), limit: limit}
}

func (s *metricCandidateSet) record(key, label, family string, cacheMiss bool) {
	candidate, ok := s.values[key]
	if !ok && len(s.values) >= s.limit {
		s.evictLeastActive()
	}
	if !ok {
		candidate.label = label
		candidate.family = family
	}
	addMetric(&candidate.stats, cacheMiss)
	s.values[key] = candidate
	heap.Push(&s.heap, metricCandidateEntry{key: key, stats: candidate.stats})
	if len(s.heap) > len(s.values)*4 {
		s.rebuildHeap()
	}
}

func (s *metricCandidateSet) evictLeastActive() {
	for len(s.heap) != 0 {
		entry := heap.Pop(&s.heap).(metricCandidateEntry)
		candidate, ok := s.values[entry.key]
		if ok && candidate.stats == entry.stats {
			delete(s.values, entry.key)
			return
		}
	}
	// The heap contains only stale entries after a previous reset.
	s.rebuildHeap()
	for key := range s.values {
		delete(s.values, key)
		return
	}
}

func (s *metricCandidateSet) rebuildHeap() {
	clear(s.heap)
	s.heap = s.heap[:0]
	for key, candidate := range s.values {
		s.heap = append(s.heap, metricCandidateEntry{key: key, stats: candidate.stats})
	}
	heap.Init(&s.heap)
}

func (s *metricCandidateSet) reset() {
	clear(s.values)
	clear(s.heap)
	s.heap = s.heap[:0]
}

// MetricClusterer collects tenant-local request and cache-miss statistics.
// Train and FreezeNormalizer before Observe so candidate keys and published
// assignments share one immutable normalization model. Call Rebalance at
// reporting-window boundaries to update its bounded dictionary, then
// ResetWindow to start the next selection window. Every observation maps to
// either an active route, an active family, or one of the fixed resource-class
// fallbacks.
type MetricClusterer struct {
	normalizer *Clusterer
	cfg        MetricConfig
	routes     metricCandidateSet
	families   metricCandidateSet
	active     map[string]MetricCluster
	fallbacks  [metricFallbackCount]MetricCluster
	total      MetricStats
	scratch    []byte
}

// NewMetricClusterer returns a tenant-scoped metric clusterer. A tenant should
// use one instance so its hard budget is not multiplied by hostname.
func NewMetricClusterer(cfg MetricConfig) *MetricClusterer {
	if cfg.MaxClusters == 0 {
		cfg.MaxClusters = 96
	}
	if cfg.MaxClusters < metricFallbackCount {
		cfg.MaxClusters = metricFallbackCount
	}
	if cfg.MaxCandidates <= 0 {
		cfg.MaxCandidates = cfg.MaxClusters * 16
		if cfg.MaxCandidates < 256 {
			cfg.MaxCandidates = 256
		}
	}
	if cfg.MinSamples == 0 {
		cfg.MinSamples = 10
	}
	if cfg.PromotionRatio < 1 || math.IsNaN(cfg.PromotionRatio) || math.IsInf(cfg.PromotionRatio, 0) {
		cfg.PromotionRatio = 1.2
	}

	m := &MetricClusterer{
		normalizer: New(cfg.Normalizer),
		cfg:        cfg,
		routes:     newMetricCandidateSet(cfg.MaxCandidates),
		families:   newMetricCandidateSet(cfg.MaxCandidates),
		active:     make(map[string]MetricCluster, cfg.MaxClusters-metricFallbackCount),
		scratch:    make([]byte, 0, 512),
	}
	for i, name := range metricFallbackNames {
		m.fallbacks[i] = MetricCluster{
			ID:    i,
			Key:   "fallback:" + name,
			Label: "other/" + name,
			Kind:  MetricFallbackCluster,
		}
	}
	return m
}

// Train updates the normalizer without recording metric statistics. Train a
// representative URL history, then call FreezeNormalizer before Observe so
// dynamic metric keys remain stable.
func (m *MetricClusterer) Train(raw []byte) {
	m.scratch = m.normalizer.Normalize(m.scratch[:0], raw)
}

// FreezeNormalizer stops normalization learning. After freezing, Observe and
// Assign use the same stable templates for route and family keys.
func (m *MetricClusterer) FreezeNormalizer() {
	m.normalizer.Freeze()
}

// Observe records one request and returns its current cluster assignment.
// Query strings are intentionally excluded from metric keys so values cannot
// become high-cardinality labels or leak into metric dimensions. Call
// FreezeNormalizer before Observe when publishing metric IDs.
func (m *MetricClusterer) Observe(raw []byte, cacheMiss bool) MetricCluster {
	m.scratch = m.normalizer.Normalize(m.scratch[:0], raw)
	route := metricRouteKey(m.scratch)
	family, fallback := metricFamily(m.scratch)

	addMetric(&m.total, cacheMiss)
	m.routes.record(route, route, family, cacheMiss)
	if family != "" {
		m.families.record(family, strings.TrimPrefix(family, "family:"), "", cacheMiss)
	}
	return m.assignment(route, family, fallback)
}

// Assign returns the current dictionary assignment without recording a new
// request. Use it to replay a training file after Rebalance.
func (m *MetricClusterer) Assign(raw []byte) MetricCluster {
	m.scratch = m.normalizer.Apply(m.scratch[:0], raw)
	route := metricRouteKey(m.scratch)
	family, fallback := metricFamily(m.scratch)
	return m.assignment(route, family, fallback)
}

func (m *MetricClusterer) assignment(route, family string, fallback int) MetricCluster {
	if cluster, ok := m.active[route]; ok {
		return cluster
	}
	if family != "" {
		if cluster, ok := m.active[family]; ok {
			return cluster
		}
	}
	return m.fallbacks[fallback]
}

// Rebalance selects the active route and family IDs from the current window.
// Existing IDs receive the configured promotion advantage to reduce churn.
func (m *MetricClusterer) Rebalance() []MetricCluster {
	if m.total.Requests == 0 {
		return m.Clusters()
	}

	dynamicBudget := m.cfg.MaxClusters - metricFallbackCount
	exactBudget := m.cfg.ExactClusters
	if exactBudget <= 0 {
		exactBudget = dynamicBudget * 2 / 3
	}
	if exactBudget > dynamicBudget {
		exactBudget = dynamicBudget
	}

	selected := make([]metricRank, 0, dynamicBudget)
	routes := m.selectCandidates(m.routes.values, MetricRouteCluster, exactBudget)
	selected = append(selected, routes...)
	selected = append(selected, m.selectCandidates(m.residualFamilies(routes), MetricFamilyCluster, dynamicBudget-exactBudget)...)

	previous := m.active
	m.active = make(map[string]MetricCluster, len(selected))
	used := make(map[int]bool, len(selected))
	for _, candidate := range selected {
		if cluster, ok := previous[candidate.key]; ok {
			m.active[candidate.key] = cluster
			used[cluster.ID] = true
		}
	}
	for _, candidate := range selected {
		if _, ok := m.active[candidate.key]; ok {
			continue
		}
		id := metricFallbackCount
		for used[id] {
			id++
		}
		used[id] = true
		m.active[candidate.key] = MetricCluster{
			ID:    id,
			Key:   candidate.key,
			Label: candidate.label,
			Kind:  candidate.kind,
		}
	}
	return m.Clusters()
}

// ResetWindow clears ranking counters while retaining the active dictionary.
// Call it after persisting the prior window's aggregate statistics.
func (m *MetricClusterer) ResetWindow() {
	m.routes.reset()
	m.families.reset()
	m.total = MetricStats{}
}

// Stats returns totals collected since the last ResetWindow call.
func (m *MetricClusterer) Stats() MetricStats { return m.total }

// Clusters returns all fixed and active metric IDs ordered by numeric ID.
func (m *MetricClusterer) Clusters() []MetricCluster {
	clusters := make([]MetricCluster, 0, metricFallbackCount+len(m.active))
	clusters = append(clusters, m.fallbacks[:]...)
	for _, cluster := range m.active {
		clusters = append(clusters, cluster)
	}
	sort.Slice(clusters, func(i, j int) bool { return clusters[i].ID < clusters[j].ID })
	return clusters
}

type metricRank struct {
	key      string
	label    string
	kind     MetricClusterKind
	priority float64
}

func (m *MetricClusterer) selectCandidates(candidates map[string]metricCandidate, kind MetricClusterKind, limit int) []metricRank {
	if limit == 0 {
		return nil
	}
	ranked := make([]metricRank, 0, len(candidates))
	for key, candidate := range candidates {
		if candidate.stats.Requests < m.cfg.MinSamples {
			continue
		}
		priority := metricScore(candidate.stats, m.total)
		if cluster, ok := m.active[key]; ok && cluster.Kind == kind {
			priority *= m.cfg.PromotionRatio
		}
		ranked = append(ranked, metricRank{key: key, label: candidate.label, kind: kind, priority: priority})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].priority == ranked[j].priority {
			return ranked[i].key < ranked[j].key
		}
		return ranked[i].priority > ranked[j].priority
	})
	if len(ranked) > limit {
		ranked = ranked[:limit]
	}
	return ranked
}

func (m *MetricClusterer) residualFamilies(routes []metricRank) map[string]metricCandidate {
	families := make(map[string]metricCandidate, len(m.families.values))
	for key, candidate := range m.families.values {
		families[key] = candidate
	}
	for _, route := range routes {
		candidate := m.routes.values[route.key]
		if candidate.family == "" {
			continue
		}
		family, ok := families[candidate.family]
		if !ok {
			continue
		}
		family.stats = subtractMetric(family.stats, candidate.stats)
		families[candidate.family] = family
	}
	return families
}

func addMetric(stats *MetricStats, cacheMiss bool) {
	if stats.Requests != math.MaxUint64 {
		stats.Requests++
	}
	if cacheMiss && stats.CacheMisses != math.MaxUint64 {
		stats.CacheMisses++
	}
}

func subtractMetric(total, part MetricStats) MetricStats {
	if part.Requests >= total.Requests {
		total.Requests = 0
	} else {
		total.Requests -= part.Requests
	}
	if part.CacheMisses >= total.CacheMisses {
		total.CacheMisses = 0
	} else {
		total.CacheMisses -= part.CacheMisses
	}
	return total
}

func metricScore(candidate, total MetricStats) float64 {
	var score float64
	if total.Requests != 0 {
		score += float64(candidate.Requests) / float64(total.Requests)
	}
	if total.CacheMisses != 0 {
		score += float64(candidate.CacheMisses) / float64(total.CacheMisses)
	}
	return score
}

func metricRouteKey(template []byte) string {
	for i, c := range template {
		if c == '?' {
			return "route:" + string(template[:i])
		}
	}
	return "route:" + string(template)
}

func metricFamily(template []byte) (string, int) {
	var parsed parsedURL
	parseURL(template, &parsed)

	fallback := metricResource(&parsed)
	first := ""
	if parsed.segmentCount > 0 {
		segment := parsed.raw[parsed.segments[0].full.start:parsed.segments[0].full.end]
		if !strings.ContainsRune(string(segment), '{') {
			first = string(segment)
		}
	}
	if first == "" {
		return "", fallback
	}
	host := "relative"
	if parsed.host.len() != 0 {
		host = strings.ToLower(string(parsed.raw[parsed.host.start:parsed.host.end]))
	}
	return "family:" + host + "/" + first + "/" + metricFallbackNames[fallback], fallback
}

func metricResource(parsed *parsedURL) int {
	if parsed.segmentCount == 0 {
		return 5 // page
	}
	first := parsed.raw[parsed.segments[0].full.start:parsed.segments[0].full.end]
	if equalFoldString(first, "api") || equalFoldString(first, "rest") || equalFoldString(first, "graphql") {
		return 0 // api
	}
	last := &parsed.segments[parsed.segmentCount-1]
	ext := strings.ToLower(string(parsed.raw[last.ext.start:last.ext.end]))
	switch ext {
	case "jpg", "jpeg", "png", "gif", "webp", "svg", "ico", "avif":
		return 1 // image
	case "js", "mjs":
		return 2 // script
	case "css":
		return 3 // style
	case "woff", "woff2", "ttf", "otf":
		return 4 // font
	case "mp4", "webm", "mp3", "m3u8":
		return 6 // media
	}
	if ext != "" && ext != "html" && ext != "htm" && ext != "xhtml" &&
		ext != "php" && ext != "asp" && ext != "aspx" && ext != "jsp" {
		return 7 // other
	}
	return 5 // page
}
