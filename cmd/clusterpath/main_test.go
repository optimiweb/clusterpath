package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/optimiweb/clusterpath/cmd/internal/stream"
)

func TestRunPipelineTwoPass(t *testing.T) {
	const input = "HTTPS://www.example.test/products/100?session=secret&sort=price\nhttps://example.test/products/101?session=other&sort=price\n"
	source := func(consume func([]byte) error) error {
		return stream.ScanLines(strings.NewReader(input), consume)
	}
	var output, report bytes.Buffer
	err := runPipeline(options{twoPass: true, maxBuckets: 16}, source, &output, &report)
	if err != nil {
		t.Fatal(err)
	}

	const template = "https://example.test/products/{id}?sort=price\n"
	if got := output.String(); got != template+template {
		t.Fatalf("output = %q, want %q", got, template+template)
	}
	if got, want := report.String(), "2\thttps://example.test/products/{id}?sort=price\n# paths=2 clusters=1 buckets=1 evictions=0\n"; got != want {
		t.Fatalf("report = %q, want %q", got, want)
	}
}

func TestRunPipelineWrapsLearningErrors(t *testing.T) {
	err := runPipeline(options{twoPass: true}, func(func([]byte) error) error {
		return errSource
	}, &bytes.Buffer{}, nil)
	if err == nil || !strings.Contains(err.Error(), "learn: source failed") {
		t.Fatalf("error = %v", err)
	}
}

func TestConfigFromOptionsRejectsOverflow(t *testing.T) {
	if uint64(^uint(0)) > uint64(^uint32(0)) {
		_, err := configFromOptions(options{minSamples: uint(uint64(^uint32(0)) + 1)})
		if err == nil || !strings.Contains(err.Error(), "-min-samples") {
			t.Fatalf("min samples error = %v", err)
		}
	}
	_, err := configFromOptions(options{distinctLimit: uint(^uint16(0)) + 1})
	if err == nil || !strings.Contains(err.Error(), "-distinct-limit") {
		t.Fatalf("distinct limit error = %v", err)
	}
}

var errSource = errors.New("source failed")
