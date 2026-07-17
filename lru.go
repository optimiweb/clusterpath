package clusterpath

type bucketCache struct {
	buckets    []bucket
	index      []int32
	mask       uint64
	count      int
	head       int32
	tail       int32
	tombstones int
	hits       uint64
	misses     uint64
	evictions  uint64
}

func newBucketCache(capacity int) bucketCache {
	indexSize := 2
	for indexSize < capacity*2 {
		indexSize <<= 1
	}
	return bucketCache{
		buckets: make([]bucket, capacity),
		index:   make([]int32, indexSize),
		mask:    uint64(indexSize - 1),
		head:    -1,
		tail:    -1,
	}
}

func (c *bucketCache) get(signature uint64, create, touch bool) *bucket {
	if slot := c.lookup(signature); slot >= 0 {
		c.hits++
		if touch {
			c.moveFront(slot)
		}
		return &c.buckets[slot]
	}
	c.misses++
	if !create {
		return nil
	}

	var slot int32
	if c.count < len(c.buckets) {
		slot = int32(c.count)
		c.count++
	} else {
		slot = c.tail
		old := &c.buckets[slot]
		c.removeIndex(old.signature)
		c.unlink(slot)
		c.evictions++
	}
	c.buckets[slot] = bucket{signature: signature, prev: -1, next: -1}
	c.insertIndex(signature, slot)
	c.pushFront(slot)
	if c.tombstones > len(c.index)/4 {
		c.rebuildIndex()
	}
	return &c.buckets[slot]
}

func (c *bucketCache) lookup(signature uint64) int32 {
	position := signature & c.mask
	for probes := 0; probes < len(c.index); probes++ {
		entry := c.index[position]
		if entry == 0 {
			return -1
		}
		if entry > 0 {
			slot := entry - 1
			if c.buckets[slot].signature == signature {
				return slot
			}
		}
		position = (position + 1) & c.mask
	}
	return -1
}

func (c *bucketCache) insertIndex(signature uint64, slot int32) {
	position := signature & c.mask
	firstTombstone := uint64(len(c.index))
	for {
		entry := c.index[position]
		if entry == -1 && firstTombstone == uint64(len(c.index)) {
			firstTombstone = position
		}
		if entry == 0 {
			if firstTombstone != uint64(len(c.index)) {
				position = firstTombstone
				c.tombstones--
			}
			c.index[position] = slot + 1
			return
		}
		position = (position + 1) & c.mask
	}
}

func (c *bucketCache) removeIndex(signature uint64) {
	position := signature & c.mask
	for {
		entry := c.index[position]
		if entry == 0 {
			return
		}
		if entry > 0 && c.buckets[entry-1].signature == signature {
			c.index[position] = -1
			c.tombstones++
			return
		}
		position = (position + 1) & c.mask
	}
}

func (c *bucketCache) rebuildIndex() {
	clear(c.index)
	c.tombstones = 0
	for slot := 0; slot < c.count; slot++ {
		c.insertIndex(c.buckets[slot].signature, int32(slot))
	}
}

func (c *bucketCache) moveFront(slot int32) {
	if c.head == slot {
		return
	}
	c.unlink(slot)
	c.pushFront(slot)
}

func (c *bucketCache) unlink(slot int32) {
	b := &c.buckets[slot]
	if b.prev >= 0 {
		c.buckets[b.prev].next = b.next
	} else {
		c.head = b.next
	}
	if b.next >= 0 {
		c.buckets[b.next].prev = b.prev
	} else {
		c.tail = b.prev
	}
	b.prev = -1
	b.next = -1
}

func (c *bucketCache) pushFront(slot int32) {
	b := &c.buckets[slot]
	b.prev = -1
	b.next = c.head
	if c.head >= 0 {
		c.buckets[c.head].prev = slot
	} else {
		c.tail = slot
	}
	c.head = slot
}
