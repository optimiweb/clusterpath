package stream

import (
	"bytes"
	"errors"
	"testing"
)

func TestScanLines(t *testing.T) {
	var lines []string
	err := ScanLines(bytes.NewBufferString("one\ntwo\n"), func(line []byte) error {
		lines = append(lines, string(line))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := lines, []string{"one", "two"}; !equalStrings(got, want) {
		t.Fatalf("lines = %q, want %q", got, want)
	}
}

func TestScanLinesReturnsConsumerError(t *testing.T) {
	want := errors.New("stop")
	err := ScanLines(bytes.NewBufferString("one\n"), func([]byte) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestOpenOutputStandard(t *testing.T) {
	var standard bytes.Buffer
	output, err := OpenOutput("-", &standard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := output.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	if got := standard.String(); got != "ok" {
		t.Fatalf("output = %q, want %q", got, "ok")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
