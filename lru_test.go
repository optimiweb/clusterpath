package clusterpath

import "testing"

func TestBucketCacheEviction(t *testing.T) {
	cache := newBucketCache(2)
	cache.get(1, true, true)
	cache.get(2, true, true)
	cache.get(1, false, true)
	cache.get(3, true, true)

	if cache.lookup(2) >= 0 {
		t.Fatal("least-recent bucket was not evicted")
	}
	if cache.lookup(1) < 0 || cache.lookup(3) < 0 {
		t.Fatal("live bucket missing after eviction")
	}
	if cache.evictions != 1 {
		t.Fatalf("evictions = %d", cache.evictions)
	}
}

func TestBucketCacheSustainedChurn(t *testing.T) {
	cache := newBucketCache(8)
	for i := uint64(1); i <= 10_000; i++ {
		cache.get(avalanche(i), true, true)
	}
	for i := uint64(9_993); i <= 10_000; i++ {
		if cache.lookup(avalanche(i)) < 0 {
			t.Fatalf("recent key %d missing", i)
		}
	}
}
