package clusterpath

import (
	"math"
	"testing"
)

func TestBitmapEstimate(t *testing.T) {
	var sketch bitmap256
	previous := uint16(0)
	for i := uint64(1); i <= 100; i++ {
		sketch.add(avalanche(i))
		current := sketch.estimate()
		if current < previous {
			t.Fatalf("estimate decreased from %d to %d", previous, current)
		}
		previous = current
	}
	if previous < 60 || previous > 150 {
		t.Fatalf("unexpected estimate %d for 100 values", previous)
	}
}

func TestTopKLowerBound(t *testing.T) {
	var top topK8
	for i := 0; i < 20; i++ {
		top.add(42)
	}
	for i := uint64(100); i < 120; i++ {
		top.add(i)
	}
	if got := top.lowerBound(42); got < 20 {
		t.Fatalf("lower bound = %d, want at least 20", got)
	}
}

func TestTopKSaturatesCounts(t *testing.T) {
	var small topK2
	small.hashes[0] = 1
	small.counts[0] = math.MaxUint32
	small.add(1)
	if small.counts[0] != math.MaxUint32 {
		t.Fatalf("topK2 count wrapped to %d", small.counts[0])
	}
	small.add(2)
	if small.hashes[0] != 1 {
		t.Fatalf("topK2 replaced saturated entry with %d", small.hashes[0])
	}

	var large topK8
	large.hashes[0] = 1
	large.counts[0] = math.MaxUint32
	large.add(1)
	if large.counts[0] != math.MaxUint32 {
		t.Fatalf("topK8 count wrapped to %d", large.counts[0])
	}
	large.add(2)
	if large.hashes[0] != 1 {
		t.Fatalf("topK8 replaced saturated entry with %d", large.hashes[0])
	}
}
