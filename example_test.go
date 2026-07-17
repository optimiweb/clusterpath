package clusterpath_test

import (
	"fmt"

	"github.com/optimiweb/clusterpath"
)

// Basic normalization: the scheme and host are canonicalized, the numeric id is
// masked, tracking parameters are dropped, functional parameters are kept and
// sorted, and the fragment is removed.
func Example() {
	c := clusterpath.New(clusterpath.DefaultConfig())

	raw := []byte("HTTPS://www.Example.TEST/products/electronics/10293?session=abc123&sort=price_asc#reviews")
	fmt.Println(string(c.Normalize(nil, raw)))
	// Output: https://example.test/products/electronics/{id}?sort=price_asc
}

// Learned masking: after enough samples share a structural shape, the
// high-cardinality leaf position collapses to a placeholder while the stable
// prefix stays literal. Freeze makes the output stable for a replay pass.
func ExampleClusterer() {
	c := clusterpath.New(clusterpath.Config{MinSamples: 4, DistinctLimit: 4})

	for i := 0; i < 8; i++ {
		c.Normalize(nil, []byte(fmt.Sprintf("/blog/article-%d.html", i)))
	}
	c.Freeze()

	fmt.Println(string(c.Apply(nil, []byte("/blog/article-99.html"))))
	// Output: /blog/article-{id}.html
}

// Reusing a destination buffer keeps Normalize allocation-free.
func ExampleClusterer_Normalize() {
	c := clusterpath.New(clusterpath.DefaultConfig())

	dst := make([]byte, 0, 256)
	for _, raw := range [][]byte{
		[]byte("/user/550e8400-e29b-41d4-a716-446655440000/orders/42"),
		[]byte("/assets/img/ca508a0b52086307ea926f194c702566.png"),
	} {
		dst = c.Normalize(dst[:0], raw)
		fmt.Println(string(dst))
	}
	// Output:
	// /user/{uuid}/orders/{id}
	// /assets/img/{hex}.png
}

// Sharding routes every URL of the same structural shape to the same worker, so
// per-shard models stay coherent while workers process independently.
func ExampleSharded() {
	s := clusterpath.NewSharded(8, clusterpath.DefaultConfig())

	a := s.Shard([]byte("/v6/users/123/newsletters/list"))
	b := s.Shard([]byte("/v6/users/999/newsletters/list"))
	fmt.Println("same shard:", a == b)

	// Each shard is owned by exactly one goroutine:
	out := s.At(a).Normalize(nil, []byte("/v6/users/123/newsletters/list"))
	fmt.Println(string(out))
	// Output:
	// same shard: true
	// /v6/users/{id}/newsletters/list
}
