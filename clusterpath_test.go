package clusterpath

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestCanonicalizeAndMaskKnownTypes(t *testing.T) {
	c := New(Config{MaxBuckets: 16})
	tests := []struct {
		input string
		want  string
	}{
		{
			"HTTPS://www.Example.TEST//products/books/88401/?session=abc&sort=price_asc#reviews",
			"https://example.test/products/books/{id}?sort=price_asc",
		},
		{
			"/resultat/candidat/ca508a0b52086307ea926f194c702566.html",
			"/resultat/candidat/{hex}.html",
		},
		{
			"/users/550e8400-e29b-41d4-a716-446655440000",
			"/users/{uuid}",
		},
		{
			"/landing/item?fbbaid=1&csp&eff_cpt=2&frz-flush=true&gad_source=1&gbraid=x&keep=yes&t=42",
			"/landing/item?keep=yes&t=42",
		},
	}
	for _, test := range tests {
		got := string(c.Normalize(make([]byte, 0, 256), []byte(test.input)))
		if got != test.want {
			t.Errorf("Normalize(%q) = %q, want %q", test.input, got, test.want)
		}
	}
}

func TestEmbeddedFingerprintClustering(t *testing.T) {
	c := New(Config{
		MaxBuckets:     16,
		MinSamples:     8,
		DistinctLimit:  8,
		HighCardRatio:  0.5,
		HeavyHitterMin: 4,
	})
	for i := 0; i < 16; i++ {
		c.Normalize(nil, []byte(fmt.Sprintf("/vstatic/0/chunks/264.chunk-%016x.js", i)))
		c.Normalize(nil, []byte(fmt.Sprintf("/vstatic/0/manifests/vis-fo_%08x-e29b-41d4-a716-446655440000.js", i)))
	}
	c.Freeze()
	for input, want := range map[string]string{
		"/vstatic/0/chunks/264.chunk-daf1ac8cb497daae.js":                     "/vstatic/{id}/chunks/264.chunk-{hash}.js",
		"/vstatic/0/manifests/vis-fo_550e8400-e29b-41d4-a716-446655440000.js": "/vstatic/{id}/manifests/vis-fo_{uuid}.js",
	} {
		if got := string(c.Apply(nil, []byte(input))); got != want {
			t.Errorf("fingerprint template %q = %q, want %q", input, got, want)
		}
	}
}

func TestOnlineLiteralClusteringAndHeavyHitter(t *testing.T) {
	c := New(Config{
		MaxBuckets:     16,
		MinSamples:     8,
		DistinctLimit:  8,
		HighCardRatio:  0.5,
		HeavyHitterMin: 3,
	})
	for i := 0; i < 30; i++ {
		c.Normalize(nil, []byte("/event"))
	}
	for i := 0; i < 30; i++ {
		value := fmt.Sprintf("/tail%c%c", 'a'+byte(i/26), 'a'+byte(i%26))
		c.Normalize(nil, []byte(value))
	}
	c.Freeze()
	if got := string(c.Apply(nil, []byte("/event"))); got != "/event" {
		t.Fatalf("heavy hitter = %q", got)
	}
	if got := string(c.Apply(nil, []byte("/tailzz"))); got != "/{slug}" {
		t.Fatalf("long-tail literal = %q", got)
	}
}

func TestCompoundSegmentsPreserveCompetingFamilies(t *testing.T) {
	c := New(Config{
		MaxBuckets:     16,
		MinSamples:     8,
		DistinctLimit:  8,
		HighCardRatio:  0.5,
		HeavyHitterMin: 4,
	})
	for i := 0; i < 16; i++ {
		c.Normalize(nil, []byte(fmt.Sprintf("/blog/article-%d.html", i)))
		c.Normalize(nil, []byte(fmt.Sprintf("/blog/post-%d.html", i)))
	}
	c.Freeze()
	for input, want := range map[string]string{
		"/blog/article-99.html": "/blog/article-{id}.html",
		"/blog/post-99.html":    "/blog/post-{id}.html",
	} {
		if got := string(c.Apply(nil, []byte(input))); got != want {
			t.Errorf("compound template %q = %q, want %q", input, got, want)
		}
	}
}

func TestQueryLearning(t *testing.T) {
	c := New(Config{
		MaxBuckets:     16,
		MinSamples:     8,
		DistinctLimit:  8,
		HighCardRatio:  0.5,
		HeavyHitterMin: 3,
	})
	for i := 0; i < 64; i++ {
		raw := fmt.Sprintf("/search?nonce=n%d&category=%c&sort=price", i, 'a'+rune(i%2))
		c.Normalize(nil, []byte(raw))
	}
	c.Freeze()
	got := string(c.Apply(nil, []byte("/search?sort=price&nonce=other&category=a&session=secret")))
	if got != "/search?category=a&nonce={value}&sort=price" {
		t.Fatalf("query normalization = %q", got)
	}
}

func TestQueryTemplateKeepsHeavyHittersAndOverrides(t *testing.T) {
	c := New(Config{
		MaxBuckets:     16,
		MinSamples:     8,
		DistinctLimit:  8,
		HighCardRatio:  0.5,
		HeavyHitterMin: 4,
		KeepParams:     []string{"cursor"},
	})
	for i := 0; i < 10; i++ {
		c.Normalize(nil, []byte("/search?cursor=stable&nonce=all"))
	}
	for i := 0; i < 54; i++ {
		c.Normalize(nil, []byte(fmt.Sprintf("/search?cursor=c%d&nonce=n%d", i, i)))
	}
	c.Freeze()
	if got := string(c.Apply(nil, []byte("/search?cursor=other&nonce=all"))); got != "/search?cursor=other&nonce=all" {
		t.Fatalf("keep/heavy values = %q", got)
	}
	if got := string(c.Apply(nil, []byte("/search?cursor=other&nonce=739"))); got != "/search?cursor=other&nonce={id}" {
		t.Fatalf("query placeholder = %q", got)
	}
}

func TestAuthorityAndQueryOverflowNormalization(t *testing.T) {
	c := New(Config{MaxBuckets: 16})
	if got := string(c.Normalize(nil, []byte("https://user:secret@WWW.Example.TEST:8443/p/12"))); got != "https://example.test:8443/p/{id}" {
		t.Fatalf("authority normalization = %q", got)
	}

	var raw strings.Builder
	raw.WriteString("/search?")
	for i := 0; i < maxParams+1; i++ {
		if i > 0 {
			raw.WriteByte('&')
		}
		fmt.Fprintf(&raw, "k%d=%d", i, i)
	}
	if got := string(c.Normalize(nil, []byte(raw.String()))); !strings.HasSuffix(got, "&{more}") {
		t.Fatalf("overflow marker missing from %q", got)
	}
}

func TestFreezeStopsLearning(t *testing.T) {
	c := New(Config{MaxBuckets: 2})
	c.Normalize(nil, []byte("/one"))
	before := c.Stats()
	c.Freeze()
	c.Normalize(nil, []byte("/unseen/deeper"))
	after := c.Stats()
	if after.Buckets != before.Buckets || after.Evictions != before.Evictions {
		t.Fatalf("freeze mutated cache: before=%+v after=%+v", before, after)
	}
}

func TestShardedStructuralRouting(t *testing.T) {
	s := NewSharded(8, Config{MaxBuckets: 4})
	a := s.Shard([]byte("/v6/users/123/newsletters/list"))
	b := s.Shard([]byte("/v6/users/999/newsletters/list"))
	if a != b {
		t.Fatalf("same structure routed to shards %d and %d", a, b)
	}
	a = s.Shard([]byte("https://www.example.test/products/123"))
	b = s.Shard([]byte("https://example.test/products/999"))
	if a != b {
		t.Fatalf("canonical hosts routed to shards %d and %d", a, b)
	}
}

func TestExampleCorpus(t *testing.T) {
	file, err := os.Open("testdata/url-clusters.txt")
	if errors.Is(err, os.ErrNotExist) {
		t.Skip("URL clustering fixture not present")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	c := New(Config{MaxBuckets: 1024, MinSamples: 16, DistinctLimit: 32})
	var paths []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		path := scanner.Text()
		paths = append(paths, path)
		c.Normalize(nil, []byte(path))
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	c.Freeze()

	clusters := make(map[string]int)
	for _, path := range paths {
		normalized := string(c.Apply(make([]byte, 0, len(path)+16), []byte(path)))
		clusters[normalized]++
	}
	for _, expected := range []string{
		"/api/tag/entity_property_values/App%5CEntity%5CEdu%5CFormation/{id}",
		"/resultat/candidat/{hex}.html",
		"/v6/users/{id}/newsletters/list",
		"/_/service_worker/66g0/sw_iframe.html",
	} {
		if clusters[expected] == 0 {
			t.Errorf("expected cluster %q not found", expected)
		}
	}
	if len(clusters) >= len(paths)/2 {
		t.Fatalf("insufficient reduction: %d paths became %d clusters", len(paths), len(clusters))
	}
	if t.Failed() {
		var examples []string
		for cluster := range clusters {
			if strings.Contains(cluster, "/api/tag/entity_property_values/") {
				examples = append(examples, cluster)
			}
		}
		t.Logf("matching API clusters: %v", examples)
	}
}

func TestScratchReuseNoStaleState(t *testing.T) {
	c := New(Config{MaxBuckets: 16})
	// A deep URL with many params, followed by shallow inputs. Because the
	// parser no longer zeroes its arrays, a reuse bug would surface as stale
	// trailing segments or parameters leaking into later outputs.
	c.Normalize(nil, []byte("/a/b/c/d/e/f?x=1&y=2&z=3"))
	cases := map[string]string{
		"/x":             "/x",
		"/one/two":       "/one/two",
		"":               "/",
		"/search?only=1": "/search?only=1",
	}
	for input, want := range cases {
		if got := string(c.Normalize(nil, []byte(input))); got != want {
			t.Errorf("after deep URL, Normalize(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestBareWildcardDropParamIgnored(t *testing.T) {
	c := New(Config{MaxBuckets: 4, DropParams: []string{"*"}})
	got := string(c.Normalize(nil, []byte("/search?a=1&b=2")))
	if got != "/search?a=1&b=2" {
		t.Fatalf("bare wildcard dropped params: %q", got)
	}
}

func TestGroupByShapeReducesClustersWithoutDestroyingEnums(t *testing.T) {
	file, err := os.Open("testdata/url-clusters.txt")
	if errors.Is(err, os.ErrNotExist) {
		t.Skip("URL clustering fixture not present")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var paths [][]byte
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		paths = append(paths, append([]byte(nil), scanner.Bytes()...))
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	count := func(cfg Config) (int, map[string]bool) {
		c := New(cfg)
		for _, p := range paths {
			c.Normalize(nil, p)
		}
		c.Freeze()
		set := make(map[string]bool)
		for _, p := range paths {
			set[string(c.Apply(nil, p))] = true
		}
		return len(set), set
	}

	baseline, _ := count(Config{MaxBuckets: 1024, MinSamples: 16, DistinctLimit: 32})
	shaped, clusters := count(Config{
		MaxBuckets: 1024, MinSamples: 8, DistinctLimit: 48,
		HighCardRatio: 0.8, SignaturePrefix: GroupByShape,
	})

	if shaped >= baseline {
		t.Fatalf("group-by-shape did not reduce clusters: baseline=%d shaped=%d", baseline, shaped)
	}
	// Bounded-enum API endpoints must survive as literals, not be masked.
	for _, keep := range []string{
		"/api/edu/formations",
		"/api/users/me",
		"/api/researcher/search/edu_formation",
	} {
		if !clusters[keep] {
			t.Errorf("group-by-shape wrongly masked enum endpoint %q", keep)
		}
	}
	// The high-cardinality leaf must still be collapsed.
	if !clusters["/etudes/annuaire-enseignement-superieur/formation/{slug}.html"] {
		t.Error("expected leaf slug cluster missing under group-by-shape")
	}
}

func TestNormalizeZeroAlloc(t *testing.T) {
	c := New(Config{MaxBuckets: 16})
	raw := []byte("https://example.test/products/books/88401?sort=price&session=secret")
	dst := make([]byte, 0, 256)
	for i := 0; i < 64; i++ {
		dst = c.Normalize(dst[:0], raw)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		dst = c.Normalize(dst[:0], raw)
	})
	if allocs != 0 {
		t.Fatalf("Normalize allocated %.2f times per call", allocs)
	}
}
