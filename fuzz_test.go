package clusterpath

import "testing"

func FuzzNormalize(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte("https://user:pw@www.Example.test:8443/a/b/c.html?x=1&y=2#frag"),
		[]byte("//host/path"),
		[]byte("/a/../b/%2F/c?=&==&#"),
		[]byte("http://[::1]:8080/x"),
		[]byte(":///?#@"),
	} {
		f.Add(seed)
	}
	c := New(Config{MaxBuckets: 64, MinSamples: 2, DistinctLimit: 4})
	dst := make([]byte, 0, 512)
	f.Fuzz(func(t *testing.T, raw []byte) {
		dst = c.Normalize(dst[:0], raw)
		dst = c.Apply(dst[:0], raw)
	})
}
