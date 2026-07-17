package clusterpath

const hashPrime uint64 = 0x100000001b3

func hashBytes(seed uint64, b []byte) uint64 {
	h := seed ^ 0xcbf29ce484222325
	for _, c := range b {
		h ^= uint64(c)
		h *= hashPrime
	}
	return avalanche(h)
}

func hashBytesFold(seed uint64, b []byte) uint64 {
	h := seed ^ 0xcbf29ce484222325
	for _, c := range b {
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		h ^= uint64(c)
		h *= hashPrime
	}
	return avalanche(h)
}

func hashAdd(h uint64, v uint64) uint64 {
	h ^= v + 0x9e3779b97f4a7c15 + h<<6 + h>>2
	return h
}

func avalanche(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}
