package clusterpath

import "math"

const (
	trackedPositions = 16
	trackedQueryKeys = 16
	ratioScale       = 10_000
)

type posStat struct {
	distinct bitmap256
	heavy    topK8
	skeleton topK2
}

type queryStat struct {
	keyHash  uint64
	count    uint32
	distinct bitmap256
	heavy    topK8
}

type bucket struct {
	signature uint64
	prev      int32
	next      int32
	total     uint32
	positions [trackedPositions]posStat
	queries   [trackedQueryKeys]queryStat
}

type decisionConfig struct {
	minSamples     uint32
	ratioThreshold uint32
	distinctLimit  uint16
	heavyMin       uint32
}

func (b *bucket) update(p *parsedURL) {
	if b.total != math.MaxUint32 {
		b.total++
	}
	limit := p.segmentCount
	if limit > trackedPositions {
		limit = trackedPositions
	}
	for i := 0; i < limit; i++ {
		s := &b.positions[i]
		s.distinct.add(p.segments[i].hash)
		s.heavy.add(p.segments[i].hash)
		if p.segments[i].hasSkeleton {
			s.skeleton.add(p.segments[i].skeleton)
		}
	}
	for i := 0; i < p.paramCount; i++ {
		q := &p.params[i]
		stat := b.query(q.keyHash, true)
		if stat == nil {
			continue
		}
		if stat.count != math.MaxUint32 {
			stat.count++
		}
		stat.distinct.add(q.valHash)
		stat.heavy.add(q.valHash)
	}
}

func (b *bucket) query(hash uint64, create bool) *queryStat {
	empty := -1
	for i := range b.queries {
		q := &b.queries[i]
		if q.count == 0 {
			if empty < 0 {
				empty = i
			}
			continue
		}
		if q.keyHash == hash {
			return q
		}
	}
	if !create || empty < 0 {
		return nil
	}
	b.queries[empty].keyHash = hash
	return &b.queries[empty]
}

func (b *bucket) maskPosition(position int, hash uint64, cfg decisionConfig) bool {
	if position >= trackedPositions || b.total < cfg.minSamples {
		return false
	}
	stat := &b.positions[position]
	if !isHighCardinality(stat.distinct.estimate(), b.total, cfg) {
		return false
	}
	return stat.heavy.lowerBound(hash) < b.heavyThreshold(cfg)
}

func (b *bucket) maskWithSkeleton(position int, s *segment, cfg decisionConfig) bool {
	if position >= trackedPositions || !s.hasSkeleton {
		return false
	}
	return b.positions[position].skeleton.lowerBound(s.skeleton) >= b.heavyThreshold(cfg)
}

func (b *bucket) templateQuery(keyHash, valueHash uint64, cfg decisionConfig) bool {
	stat := b.query(keyHash, false)
	return stat != nil && stat.count >= cfg.minSamples &&
		isHighCardinality(stat.distinct.estimate(), stat.count, cfg) &&
		stat.heavy.lowerBound(valueHash) < queryHeavyThreshold(stat.count, cfg)
}

func (b *bucket) heavyThreshold(cfg decisionConfig) uint32 {
	return queryHeavyThreshold(b.total, cfg)
}

func queryHeavyThreshold(total uint32, cfg decisionConfig) uint32 {
	threshold := cfg.heavyMin
	if share := total / 64; share > threshold {
		threshold = share
	}
	return threshold
}

func isHighCardinality(distinct uint16, total uint32, cfg decisionConfig) bool {
	if distinct >= cfg.distinctLimit {
		return true
	}
	return uint64(distinct)*ratioScale >= uint64(total)*uint64(cfg.ratioThreshold)
}
