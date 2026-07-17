package clusterpath

type tokenClass uint8

const (
	classLiteral tokenClass = iota
	classNumber
	classHex
	classUUID
	classRandom
)

func analyzeParsed(p *parsedURL, seed uint64, computeValues bool) {
	for i := 0; i < p.segmentCount; i++ {
		s := &p.segments[i]
		stemLen := s.stem.end - s.stem.start
		s.class, s.hash = classifyAndHash(p.raw[s.full.start:s.full.end], stemLen, seed)
		s.skeleton, s.hasSkeleton = segmentSkeletonHash(p.raw, s.stem, seed)
		s.hasFingerprint = hasSegmentFingerprint(p.raw, s.stem, s.ext)
	}
	for i := 0; i < p.paramCount; i++ {
		q := &p.params[i]
		q.keyHash = hashBytesFold(seed, p.raw[q.key.start:q.key.end])
		if computeValues {
			q.valHash = hashBytes(seed^q.keyHash, p.raw[q.value.start:q.value.end])
		}
	}
}

// hasSegmentFingerprint recognizes build fingerprints in static assets. The
// extension guard prevents product slugs that happen to end in hexadecimal text
// from being split into a more specific template.
func hasSegmentFingerprint(raw []byte, stem, ext span) bool {
	start, class, ok := fingerprintSuffix(raw, stem)
	if !ok {
		return false
	}
	if ext.len() != 0 && isBuildExtension(raw[ext.start:ext.end]) {
		return true
	}
	return class == classUUID && hasASCIIPrefix(raw[stem.start:start], "vis-fo_")
}

func fingerprintSuffix(raw []byte, stem span) (int, tokenClass, bool) {
	if stem.len() <= 1 {
		return 0, classLiteral, false
	}
	if stem.len() > 36 {
		start := stem.end - 36
		if isUUID(raw[start:stem.end]) && isFingerprintDelimiter(raw[start-1]) {
			return start, classUUID, true
		}
	}
	start := stem.end
	for start > stem.start && isHex(raw[start-1]) {
		start--
	}
	if stem.end-start >= 12 && start > stem.start && isFingerprintDelimiter(raw[start-1]) {
		return start, classHex, true
	}
	return 0, classLiteral, false
}

func isFingerprintDelimiter(c byte) bool {
	return c == '-' || c == '_' || c == '.'
}

func isBuildExtension(ext []byte) bool {
	return equalFoldString(ext, "js") || equalFoldString(ext, "css") ||
		equalFoldString(ext, "woff") || equalFoldString(ext, "woff2")
}

func hasASCIIPrefix(b []byte, prefix string) bool {
	if len(b) < len(prefix) {
		return false
	}
	return equalFoldString(b[:len(prefix)], prefix)
}

// segmentSkeletonHash recognizes stable literal text around decimal runs without
// retaining any input bytes. It deliberately accepts only URL-safe ASCII so
// percent-encoded data and opaque tokens continue through the normal classifier.
func segmentSkeletonHash(raw []byte, stem span, seed uint64) (uint64, bool) {
	h := seed ^ 0x9e3779b97f4a7c15
	hasDigits, hasLiteral, inDigits := false, false, false
	for _, c := range raw[stem.start:stem.end] {
		switch {
		case isDigit(c):
			if !inDigits {
				h ^= 0
				h *= hashPrime
			}
			hasDigits = true
			inDigits = true
		case isASCIILetter(c) || c == '-' || c == '_':
			h ^= uint64(c)
			h *= hashPrime
			hasLiteral = true
			inDigits = false
		default:
			return 0, false
		}
	}
	return avalanche(h), hasDigits && hasLiteral
}

// classifyAndHash classifies a path segment and computes its FNV hash in a
// single pass over the segment bytes. full is the whole segment (stem plus any
// extension); only the first stemLen bytes influence the class, but the hash
// covers every byte (identical to hashBytes over full). Merging the two passes
// removes a full re-scan per segment on the hot path.
func classifyAndHash(full []byte, stemLen int, seed uint64) (tokenClass, uint64) {
	h := seed ^ 0xcbf29ce484222325

	digits, letters, separators, transitions := 0, 0, 0, 0
	checkHex := stemLen >= 16
	allHex := checkHex
	lastKind := byte(0)

	for i := 0; i < len(full); i++ {
		c := full[i]
		h ^= uint64(c)
		h *= hashPrime
		if i >= stemLen {
			continue
		}
		switch {
		case c >= '0' && c <= '9':
			if lastKind == 2 {
				transitions++
			}
			digits++
			lastKind = 1
		case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			if checkHex && allHex {
				lc := c | 0x20
				if lc > 'f' {
					allHex = false
				}
			}
			if lastKind == 1 {
				transitions++
			}
			letters++
			lastKind = 2
		case c == '-' || c == '_' || c == '%' || c == '+' || c == ',' || c == '~':
			separators++
		default:
			allHex = false
		}
	}
	h = avalanche(h)

	if stemLen == 0 {
		return classLiteral, h
	}
	if stemLen == 36 && isUUID(full[:stemLen]) {
		return classUUID, h
	}
	if digits == stemLen {
		return classNumber, h
	}
	if checkHex && allHex && digits > 0 {
		return classHex, h
	}
	if stemLen > 128 {
		return classRandom, h
	}
	if digits > 0 && letters > 0 && separators == 0 {
		if stemLen >= 5 && transitions >= 2 {
			return classRandom, h
		}
		if stemLen >= 12 && digits+letters == stemLen {
			return classRandom, h
		}
	}
	return classLiteral, h
}

// classify reports the token class of a standalone segment. It is a thin
// wrapper over classifyAndHash used by tests and callers that do not need the
// hash.
func classify(b []byte) tokenClass {
	class, _ := classifyAndHash(b, len(b), 0)
	return class
}

func isUUID(b []byte) bool {
	if len(b) != 36 {
		return false
	}
	for i, c := range b {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !isHex(c) {
			return false
		}
	}
	return true
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isASCIILetter(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isHex(c byte) bool {
	return isDigit(c) || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}
