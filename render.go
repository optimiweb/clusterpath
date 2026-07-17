package clusterpath

func (c *Clusterer) render(dst []byte, p *parsedURL, b *bucket) []byte {
	if p.hasScheme {
		dst = appendLower(dst, p.raw[p.scheme.start:p.scheme.end])
		dst = append(dst, ':', '/', '/')
	} else if p.protocolRel {
		dst = append(dst, '/', '/')
	}
	if p.host.len() != 0 {
		host := p.raw[p.host.start:p.host.end]
		if len(host) >= 4 && equalFoldString(host[:4], "www.") {
			host = host[4:]
		}
		dst = appendLower(dst, host)
	}
	if p.port.len() != 0 {
		dst = append(dst, ':')
		dst = append(dst, p.raw[p.port.start:p.port.end]...)
	}

	if p.segmentCount == 0 {
		dst = append(dst, '/')
	} else {
		for i := 0; i < p.segmentCount; i++ {
			dst = append(dst, '/')
			s := &p.segments[i]
			if s.class == classLiteral && s.hasFingerprint {
				dst = appendSegmentFingerprint(dst, p.raw, s.stem)
				if s.ext.len() != 0 {
					dst = append(dst, '.')
					dst = appendLower(dst, p.raw[s.ext.start:s.ext.end])
				}
				continue
			}
			maskLiteral := s.class == classLiteral && b != nil &&
				b.maskPosition(i, s.hash, c.decisions)
			if s.class == classLiteral && !maskLiteral {
				dst = append(dst, p.raw[s.full.start:s.full.end]...)
				continue
			}
			if s.class == classLiteral && b != nil &&
				b.maskWithSkeleton(i, s, c.decisions) {
				dst = appendSegmentSkeleton(dst, p.raw, s.stem)
				if s.ext.len() != 0 {
					dst = append(dst, '.')
					dst = appendLower(dst, p.raw[s.ext.start:s.ext.end])
				}
				continue
			}
			switch s.class {
			case classNumber:
				dst = append(dst, "{id}"...)
			case classHex:
				dst = append(dst, "{hex}"...)
			case classUUID:
				dst = append(dst, "{uuid}"...)
			case classRandom:
				dst = append(dst, "{token}"...)
			default:
				dst = append(dst, "{slug}"...)
			}
			if s.ext.len() != 0 {
				dst = append(dst, '.')
				dst = appendLower(dst, p.raw[s.ext.start:s.ext.end])
			}
		}
	}
	if p.segmentOverflow {
		dst = append(dst, "/{more}"...)
	}

	var order [maxParams]uint8
	kept := 0
	for i := 0; i < p.paramCount; i++ {
		q := &p.params[i]
		if c.keep.matches(p.raw, q.key) {
			order[kept] = uint8(i)
			kept++
			continue
		}
		if c.drop.matches(p.raw, q.key) {
			continue
		}
		order[kept] = uint8(i)
		kept++
	}
	for i := 1; i < kept; i++ {
		value := order[i]
		j := i
		for j > 0 && compareSpans(p.raw, p.params[value].key, p.params[order[j-1]].key) < 0 {
			order[j] = order[j-1]
			j--
		}
		order[j] = value
	}
	for i := 0; i < kept; i++ {
		if i == 0 {
			dst = append(dst, '?')
		} else {
			dst = append(dst, '&')
		}
		q := &p.params[order[i]]
		dst = append(dst, p.raw[q.key.start:q.key.end]...)
		if q.hasValue {
			dst = append(dst, '=')
			if !c.keep.matches(p.raw, q.key) && b != nil &&
				b.templateQuery(q.keyHash, q.valHash, c.decisions) {
				dst = appendQueryPlaceholder(dst, p.raw[q.value.start:q.value.end])
			} else {
				dst = append(dst, p.raw[q.value.start:q.value.end]...)
			}
		}
	}
	if p.paramOverflow {
		if kept == 0 {
			dst = append(dst, '?')
		} else {
			dst = append(dst, '&')
		}
		dst = append(dst, "{more}"...)
	}
	return dst
}

func appendSegmentFingerprint(dst, raw []byte, stem span) []byte {
	start, class, _ := fingerprintSuffix(raw, stem)
	dst = append(dst, raw[stem.start:start]...)
	switch class {
	case classUUID:
		return append(dst, "{uuid}"...)
	default:
		return append(dst, "{hash}"...)
	}
}

func appendSegmentSkeleton(dst, raw []byte, stem span) []byte {
	inDigits := false
	for _, c := range raw[stem.start:stem.end] {
		if isDigit(c) {
			if !inDigits {
				dst = append(dst, "{id}"...)
				inDigits = true
			}
			continue
		}
		dst = append(dst, c)
		inDigits = false
	}
	return dst
}

func appendQueryPlaceholder(dst, value []byte) []byte {
	class, _ := classifyAndHash(value, len(value), 0)
	switch class {
	case classNumber:
		return append(dst, "{id}"...)
	case classHex:
		return append(dst, "{hex}"...)
	case classUUID:
		return append(dst, "{uuid}"...)
	case classRandom:
		return append(dst, "{token}"...)
	default:
		return append(dst, "{value}"...)
	}
}

func appendLower(dst, source []byte) []byte {
	for _, c := range source {
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		dst = append(dst, c)
	}
	return dst
}

func compareSpans(raw []byte, a, b span) int {
	n := a.len()
	if b.len() < n {
		n = b.len()
	}
	for i := 0; i < n; i++ {
		ac := raw[a.start+i]
		bc := raw[b.start+i]
		if ac < bc {
			return -1
		}
		if ac > bc {
			return 1
		}
	}
	return a.len() - b.len()
}
