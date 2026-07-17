package clusterpath

import "testing"

func TestParseURL(t *testing.T) {
	raw := []byte("HTTPS://WWW.Example.TEST//products/books/88401/?b=2&a=1#reviews")
	var parsed parsedURL
	if !parseURL(raw, &parsed) {
		t.Fatal("parseURL failed")
	}
	if got := string(raw[parsed.scheme.start:parsed.scheme.end]); got != "HTTPS" {
		t.Fatalf("scheme = %q", got)
	}
	if got := string(raw[parsed.host.start:parsed.host.end]); got != "WWW.Example.TEST" {
		t.Fatalf("host = %q", got)
	}
	if parsed.segmentCount != 3 || parsed.paramCount != 2 {
		t.Fatalf("segments=%d params=%d", parsed.segmentCount, parsed.paramCount)
	}
	if got := string(raw[parsed.segments[2].full.start:parsed.segments[2].full.end]); got != "88401" {
		t.Fatalf("last segment = %q", got)
	}
}

func TestParseAuthoritySeparatesHostPortAndUserinfo(t *testing.T) {
	raw := []byte("https://user:secret@WWW.Example.TEST:8443/path")
	var parsed parsedURL
	parseURL(raw, &parsed)
	if got := string(raw[parsed.host.start:parsed.host.end]); got != "WWW.Example.TEST" {
		t.Fatalf("host = %q", got)
	}
	if got := string(raw[parsed.port.start:parsed.port.end]); got != "8443" {
		t.Fatalf("port = %q", got)
	}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		value string
		want  tokenClass
	}{
		{"12345", classNumber},
		{"ca508a0b52086307ea926f194c702566", classHex},
		{"550e8400-e29b-41d4-a716-446655440000", classUUID},
		{"mgff5v57usgdu3w", classRandom},
		{"vzkv3e", classRandom},
		{"43icxoshgjnh", classRandom},    // digits clustered at front, few transitions
		{"1azayhtvfgtjfxk", classRandom}, // leading digit run
		{"formation-master-2026", classLiteral},
		{"bac2024", classLiteral}, // short mixed token stays literal (slug-like)
		{"iphone13", classLiteral},
		{"event", classLiteral},
	}
	for _, test := range tests {
		if got := classify([]byte(test.value)); got != test.want {
			t.Errorf("classify(%q) = %d, want %d", test.value, got, test.want)
		}
	}
}

func TestSplitExtension(t *testing.T) {
	raw := []byte("ca508a0b52086307ea926f194c702566.html")
	stem, ext := splitExtension(raw, span{0, len(raw)})
	if got := string(raw[stem.start:stem.end]); got != "ca508a0b52086307ea926f194c702566" {
		t.Fatalf("stem = %q", got)
	}
	if got := string(raw[ext.start:ext.end]); got != "html" {
		t.Fatalf("ext = %q", got)
	}
}

func TestBuildFingerprintDetection(t *testing.T) {
	for _, test := range []struct {
		value string
		want  bool
	}{
		{"264.chunk-daf1ac8cb497daae.js", true},
		{"vis-fo_550e8400-e29b-41d4-a716-446655440000", true},
		{"product-deadbeefdeadbeef.html", false},
	} {
		raw := []byte(test.value)
		stem, ext := splitExtension(raw, span{0, len(raw)})
		if got := hasSegmentFingerprint(raw, stem, ext); got != test.want {
			t.Errorf("hasSegmentFingerprint(%q) = %t, want %t", test.value, got, test.want)
		}
	}
}
