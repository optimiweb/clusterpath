package clusterpath

import (
	"math"
	"strings"
	"testing"
)

func TestMetricClustererUsesHardBudgetAndFallbacks(t *testing.T) {
	m := NewMetricClusterer(MetricConfig{
		MaxClusters:   10,
		ExactClusters: 1,
		MinSamples:    1,
	})
	for i := 0; i < 20; i++ {
		m.Observe([]byte("https://shop.example/offer/buy/123?token=secret"), i%2 == 0)
	}
	for i := 0; i < 10; i++ {
		m.Observe([]byte("https://shop.example/search/123?q=private"), false)
	}

	clusters := m.Rebalance()
	if len(clusters) > 10 {
		t.Fatalf("clusters = %d, want at most 10", len(clusters))
	}
	if clusters[0].Key != "fallback:api" || clusters[7].Key != "fallback:other" {
		t.Fatalf("fixed fallback IDs changed: %#v", clusters[:8])
	}
	for _, cluster := range clusters {
		if strings.Contains(cluster.Label, "secret") || strings.Contains(cluster.Label, "private") {
			t.Fatalf("metric label retained query value: %#v", cluster)
		}
	}
	assignment := m.Observe([]byte("https://shop.example/offer/buy/999?token=other"), false)
	if assignment.Kind != MetricRouteCluster {
		t.Fatalf("offer assignment kind = %d, want route", assignment.Kind)
	}
	stats := m.Stats()
	if stats.Requests != 31 || stats.CacheMisses != 10 {
		t.Fatalf("stats = %+v, want 31 requests and 10 misses", stats)
	}
}

func TestMetricClustererPrioritizesCacheMisses(t *testing.T) {
	m := NewMetricClusterer(MetricConfig{
		MaxClusters:   9,
		ExactClusters: 1,
		MinSamples:    1,
	})
	for i := 0; i < 100; i++ {
		m.Observe([]byte("/popular/123"), false)
	}
	for i := 0; i < 10; i++ {
		m.Observe([]byte("/failing/123"), true)
	}
	m.Rebalance()

	assignment := m.Observe([]byte("/failing/999"), false)
	if assignment.Kind != MetricRouteCluster || !strings.Contains(assignment.Label, "/failing/{id}") {
		t.Fatalf("failing route assignment = %#v", assignment)
	}
}

func TestMetricClustererRebalancesAcrossWindows(t *testing.T) {
	m := NewMetricClusterer(MetricConfig{
		MaxClusters:   9,
		ExactClusters: 1,
		MinSamples:    1,
	})
	for i := 0; i < 20; i++ {
		m.Observe([]byte("/first/123"), false)
	}
	m.Rebalance()
	if got := m.Observe([]byte("/first/999"), false); !strings.Contains(got.Label, "/first/{id}") {
		t.Fatalf("first window assignment = %#v", got)
	}

	m.ResetWindow()
	for i := 0; i < 30; i++ {
		m.Observe([]byte("/second/123"), false)
	}
	m.Rebalance()
	if got := m.Observe([]byte("/second/999"), false); !strings.Contains(got.Label, "/second/{id}") {
		t.Fatalf("second window assignment = %#v", got)
	}
}

func TestMetricClustererAdmitsLateHeavyCandidates(t *testing.T) {
	m := NewMetricClusterer(MetricConfig{
		MaxClusters:   9,
		ExactClusters: 1,
		MaxCandidates: 1,
		MinSamples:    2,
	})
	m.Observe([]byte("/cold/123"), false)
	for i := 0; i < 10; i++ {
		m.Observe([]byte("/late/123"), i%2 == 0)
	}
	if len(m.routes.values) != 1 || len(m.families.values) != 1 {
		t.Fatalf("candidate maps exceeded budget: routes=%d families=%d", len(m.routes.values), len(m.families.values))
	}
	m.Rebalance()
	if got := m.Observe([]byte("/late/999"), false); got.Kind != MetricRouteCluster {
		t.Fatalf("late heavy route assignment = %#v", got)
	}
}

func TestMetricClustererUsesFamiliesForResidualTraffic(t *testing.T) {
	m := NewMetricClusterer(MetricConfig{
		MaxClusters:   10,
		ExactClusters: 1,
		MinSamples:    1,
	})
	for i := 0; i < 20; i++ {
		m.Observe([]byte("/assets/a/123.js"), false)
	}
	for i := 0; i < 10; i++ {
		m.Observe([]byte("/assets/b/123.js"), false)
	}
	m.Rebalance()

	if got := m.Observe([]byte("/assets/b/999.js"), false); got.Kind != MetricFamilyCluster {
		t.Fatalf("residual route assignment = %#v, want family", got)
	}
}

func TestMetricClustererKeepsDictionaryOnEmptyWindow(t *testing.T) {
	m := NewMetricClusterer(MetricConfig{
		MaxClusters:   9,
		ExactClusters: 1,
		MinSamples:    1,
	})
	for i := 0; i < 10; i++ {
		m.Observe([]byte("/products/123"), false)
	}
	m.Rebalance()
	m.ResetWindow()
	m.Rebalance()

	if got := m.Assign([]byte("/products/456")); got.Kind != MetricRouteCluster {
		t.Fatalf("assignment after empty rebalance = %#v, want route", got)
	}
}

func TestMetricFamilySeparatesRelativeAndHostedURLs(t *testing.T) {
	relative, _ := metricFamily([]byte("/foo/a"))
	hosted, _ := metricFamily([]byte("https://path/foo/b"))
	if relative == hosted {
		t.Fatalf("relative and hosted families collided: %q", relative)
	}
}

func TestMetricResourceUsesOtherForUnknownExtensions(t *testing.T) {
	var parsed parsedURL
	parseURL([]byte("/download/archive.wasm"), &parsed)
	if got := metricResource(&parsed); got != 7 {
		t.Fatalf("unknown extension fallback = %d, want 7", got)
	}
	parseURL([]byte("/article.html"), &parsed)
	if got := metricResource(&parsed); got != 5 {
		t.Fatalf("HTML fallback = %d, want 5", got)
	}
}

func TestMetricPromotionRatioAllowsDisabledHysteresis(t *testing.T) {
	disabled := NewMetricClusterer(MetricConfig{PromotionRatio: 1})
	if disabled.cfg.PromotionRatio != 1 {
		t.Fatalf("promotion ratio = %v, want 1", disabled.cfg.PromotionRatio)
	}
	invalid := NewMetricClusterer(MetricConfig{PromotionRatio: math.NaN()})
	if invalid.cfg.PromotionRatio != 1.2 {
		t.Fatalf("NaN promotion ratio = %v, want 1.2", invalid.cfg.PromotionRatio)
	}
}

func TestMetricClustererTrainsFrozenTemplates(t *testing.T) {
	m := NewMetricClusterer(MetricConfig{
		MaxClusters:   9,
		ExactClusters: 1,
		MinSamples:    1,
		Normalizer: Config{
			MinSamples:      2,
			DistinctLimit:   2,
			SignaturePrefix: GroupByShape,
		},
	})
	history := [][]byte{
		[]byte("/blog/alpha"),
		[]byte("/blog/bravo"),
		[]byte("/blog/charlie"),
		[]byte("/blog/delta"),
	}
	for _, raw := range history {
		m.Train(raw)
	}
	m.FreezeNormalizer()
	if got := m.Stats(); got != (MetricStats{}) {
		t.Fatalf("training recorded metric stats: %+v", got)
	}
	for _, raw := range history {
		m.Observe(raw, false)
	}
	m.Rebalance()

	if got := m.Assign([]byte("/blog/echo")); got.Kind != MetricRouteCluster || !strings.Contains(got.Label, "/blog/{slug}") {
		t.Fatalf("frozen assignment = %#v, want normalized route", got)
	}
}
