package clusterpath

func structuralSignature(p *parsedURL, seed uint64, literalPrefix int) uint64 {
	h := seed ^ 0x6a09e667f3bcc909
	if p.host.len() != 0 {
		host := p.raw[p.host.start:p.host.end]
		if len(host) >= 4 && equalFoldString(host[:4], "www.") {
			host = host[4:]
		}
		h = hashAdd(h, hashBytesFold(seed, host))
	}
	if p.port.len() != 0 {
		h = hashAdd(h, hashBytes(seed, p.raw[p.port.start:p.port.end]))
	}
	h = hashAdd(h, uint64(p.segmentCount))
	if p.segmentOverflow {
		h = hashAdd(h, 0xffffffffffffffff)
	}
	for i := 0; i < p.segmentCount; i++ {
		s := &p.segments[i]
		shape := uint64(s.class) + 1
		if s.ext.len() != 0 {
			shape |= 1 << 8
			shape = hashAdd(shape, hashBytesFold(seed, p.raw[s.ext.start:s.ext.end]))
		}
		h = hashAdd(h, shape)
		if p.segmentCount > 1 && i < literalPrefix && s.class == classLiteral {
			h = hashAdd(h, s.hash)
		}
	}
	return avalanche(h)
}
