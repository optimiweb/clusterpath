package clusterpath

import (
	"math"
	"math/bits"
)

type bitmap256 [4]uint64

var cardinalityEstimate [257]uint16

func init() {
	for occupied := 0; occupied < 256; occupied++ {
		estimate := -256 * math.Log(float64(256-occupied)/256)
		if estimate > math.MaxUint16 {
			estimate = math.MaxUint16
		}
		cardinalityEstimate[occupied] = uint16(estimate + 0.5)
	}
	cardinalityEstimate[256] = math.MaxUint16
}

func (b *bitmap256) add(hash uint64) {
	bit := uint8(hash)
	b[bit>>6] |= uint64(1) << (bit & 63)
}

func (b *bitmap256) estimate() uint16 {
	occupied := bits.OnesCount64(b[0]) + bits.OnesCount64(b[1]) +
		bits.OnesCount64(b[2]) + bits.OnesCount64(b[3])
	return cardinalityEstimate[occupied]
}

type topK8 struct {
	hashes [8]uint64
	counts [8]uint32
	errors [8]uint32
}

// topK2 is a compact Space-Saving summary for dominant segment skeletons. Two
// candidates preserve distinct route families while keeping bucket memory fixed.
type topK2 struct {
	hashes [2]uint64
	counts [2]uint32
	errors [2]uint32
}

func (t *topK2) add(hash uint64) {
	empty := -1
	minimum := 0
	for i := range t.hashes {
		if t.counts[i] == 0 {
			if empty < 0 {
				empty = i
			}
			continue
		}
		if t.hashes[i] == hash {
			if t.counts[i] != math.MaxUint32 {
				t.counts[i]++
			}
			return
		}
		if t.counts[i] < t.counts[minimum] || t.counts[minimum] == 0 {
			minimum = i
		}
	}
	if empty >= 0 {
		t.hashes[empty] = hash
		t.counts[empty] = 1
		t.errors[empty] = 0
		return
	}
	old := t.counts[minimum]
	if old == math.MaxUint32 {
		return
	}
	t.hashes[minimum] = hash
	t.errors[minimum] = old
	t.counts[minimum] = old + 1
}

func (t *topK2) lowerBound(hash uint64) uint32 {
	for i := range t.hashes {
		if t.counts[i] != 0 && t.hashes[i] == hash {
			return t.counts[i] - t.errors[i]
		}
	}
	return 0
}

func (t *topK8) add(hash uint64) {
	empty := -1
	minimum := 0
	for i := range t.hashes {
		if t.counts[i] == 0 {
			if empty < 0 {
				empty = i
			}
			continue
		}
		if t.hashes[i] == hash {
			if t.counts[i] != math.MaxUint32 {
				t.counts[i]++
			}
			return
		}
		if t.counts[i] < t.counts[minimum] || t.counts[minimum] == 0 {
			minimum = i
		}
	}
	if empty >= 0 {
		t.hashes[empty] = hash
		t.counts[empty] = 1
		t.errors[empty] = 0
		return
	}
	old := t.counts[minimum]
	if old == math.MaxUint32 {
		return
	}
	t.hashes[minimum] = hash
	t.errors[minimum] = old
	t.counts[minimum] = old + 1
}

func (t *topK8) lowerBound(hash uint64) uint32 {
	for i := range t.hashes {
		if t.counts[i] != 0 && t.hashes[i] == hash {
			return t.counts[i] - t.errors[i]
		}
	}
	return 0
}
