package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseRecord(t *testing.T) {
	url, miss, err := parseRecord([]byte("https://store.example/p/42\tmiss"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(url) != "https://store.example/p/42" || !miss {
		t.Fatalf("parseRecord = %q, %t", url, miss)
	}
	if _, _, err := parseRecord([]byte("https://store.example/p/42\tmaybe"), 1); err == nil {
		t.Fatal("invalid cache-miss value was accepted")
	}
}

type flushFailWriter struct{}

func (flushFailWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestWriteReportReturnsFlushError(t *testing.T) {
	if err := writeReport(flushFailWriter{}, nil, nil); err == nil {
		t.Fatal("writeReport accepted a flush failure")
	}
}

func TestRunCollectsCandidatesWithFrozenNormalizer(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "urls.txt")
	output := filepath.Join(dir, "report.tsv")
	var urls strings.Builder
	for i := 0; i < 40; i++ {
		fmt.Fprintf(&urls, "/articles/slug-%d\n", i)
	}
	if err := os.WriteFile(input, []byte(urls.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(options{
		input:           input,
		output:          output,
		maxClusters:     9,
		exactClusters:   1,
		minSamples:      10,
		cacheMissColumn: -1,
	}); err != nil {
		t.Fatal(err)
	}
	report, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(report), "\troute\t40\t0\t") {
		t.Fatalf("report did not select the frozen route template:\n%s", report)
	}
}

func TestRunPipeline(t *testing.T) {
	input := bytes.NewReader([]byte("/products/100\tmiss\n/products/101\thit\n"))
	var output bytes.Buffer
	err := runPipeline(options{
		maxClusters:     9,
		exactClusters:   1,
		minSamples:      1,
		cacheMissColumn: 1,
	}, input, &output)
	if err != nil {
		t.Fatal(err)
	}
	report := output.String()
	if !strings.Contains(report, "cluster_id\tkind\trequests\tcache_misses") {
		t.Fatalf("missing report header:\n%s", report)
	}
	if !strings.Contains(report, "\troute\t2\t1\t") {
		t.Fatalf("missing route totals:\n%s", report)
	}
	if !strings.Contains(report, "# requests=2 cache_misses=1") {
		t.Fatalf("missing summary:\n%s", report)
	}
}

func TestRunWindowedPipelineKeepsDictionaryVersionsSeparate(t *testing.T) {
	input := strings.NewReader("/products/100\n/products/101\n/products/102\n/products/103\n")
	var output bytes.Buffer
	err := runWindowedPipeline(options{
		maxClusters:     9,
		exactClusters:   1,
		minSamples:      1,
		cacheMissColumn: -1,
		windowSize:      2,
	}, input, &output)
	if err != nil {
		t.Fatal(err)
	}
	report := output.String()
	if !strings.Contains(report, "dictionary_version\tcluster_id\tkind\thits") {
		t.Fatalf("missing streaming report header:\n%s", report)
	}
	if !strings.Contains(report, "1\t5\tfallback\t2\t0\t") {
		t.Fatalf("first window was not assigned to the initial fallback dictionary:\n%s", report)
	}
	if !strings.Contains(report, "2\t8\troute\t2\t0\t0.000000\troute:/products/{id}") {
		t.Fatalf("second window was not assigned to the rebalanced route dictionary:\n%s", report)
	}
	if !strings.Contains(report, "# requests=4 cache_misses=0 dictionary_versions=2") {
		t.Fatalf("unexpected streaming summary:\n%s", report)
	}
}
