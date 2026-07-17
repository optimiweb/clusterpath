package main

import (
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
