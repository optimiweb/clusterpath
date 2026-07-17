package clusterpath

import "testing"

func BenchmarkNormalize(b *testing.B) {
	c := New(Config{MaxBuckets: 1024})
	raw := []byte("https://example.test/products/electronics/10293?category=tvs&sort=price_asc&session=abc123")
	dst := make([]byte, 0, 256)
	for i := 0; i < 64; i++ {
		dst = c.Normalize(dst[:0], raw)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = c.Normalize(dst[:0], raw)
	}
	_ = dst
}

func BenchmarkApplyFrozen(b *testing.B) {
	c := New(Config{MaxBuckets: 1024})
	raw := []byte("https://example.test/products/electronics/10293?category=tvs&sort=price_asc&session=abc123")
	dst := make([]byte, 0, 256)
	for i := 0; i < 64; i++ {
		dst = c.Normalize(dst[:0], raw)
	}
	c.Freeze()
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = c.Apply(dst[:0], raw)
	}
	_ = dst
}
