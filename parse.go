package clusterpath

const (
	maxSegments = 32
	maxParams   = 32
)

type span struct {
	start int
	end   int
}

func (s span) len() int { return s.end - s.start }

type segment struct {
	full           span
	stem           span
	ext            span
	class          tokenClass
	hash           uint64
	skeleton       uint64
	hasSkeleton    bool
	hasFingerprint bool
}

type queryParam struct {
	key      span
	value    span
	hasValue bool
	keyHash  uint64
	valHash  uint64
}

type parsedURL struct {
	raw             []byte
	scheme          span
	host            span
	port            span
	segments        [maxSegments]segment
	params          [maxParams]queryParam
	segmentCount    int
	paramCount      int
	hasScheme       bool
	protocolRel     bool
	segmentOverflow bool
	paramOverflow   bool
}

func parseURL(raw []byte, p *parsedURL) bool {
	// Reset scalar fields only. The segments/params arrays are written up to
	// segmentCount/paramCount before they are read, so zeroing the full
	// (multi-kilobyte) struct on every call is unnecessary.
	p.raw = raw
	p.scheme = span{}
	p.host = span{}
	p.port = span{}
	p.segmentCount = 0
	p.paramCount = 0
	p.hasScheme = false
	p.protocolRel = false
	p.segmentOverflow = false
	p.paramOverflow = false
	if len(raw) == 0 {
		return true
	}

	pathStart := 0
	if len(raw) >= 2 && raw[0] == '/' && raw[1] == '/' {
		p.protocolRel = true
		pathStart = parseAuthority(raw, 2, p)
	} else if schemeEnd := findScheme(raw, len(raw)); schemeEnd >= 0 {
		p.hasScheme = true
		p.scheme = span{0, schemeEnd}
		pathStart = parseAuthority(raw, schemeEnd+3, p)
	}

	// Tokenize path segments and detect the query ('?') / fragment ('#')
	// boundary in a single pass. Each segment is scanned once, tracking the
	// last '.' and whether every following byte is alphanumeric, so the
	// extension is found without a separate backward pass.
	end := len(raw)
	queryStart := len(raw)
	i := pathStart
	for i < len(raw) {
		c := raw[i]
		if c == '/' {
			i++
			continue
		}
		if c == '?' {
			queryStart = i
			for j := i + 1; j < len(raw); j++ {
				if raw[j] == '#' {
					end = j
					break
				}
			}
			break
		}
		if c == '#' {
			end = i
			break
		}

		start := i
		dot := -1
		extValid := false
		for i < len(raw) {
			c = raw[i]
			if c == '/' || c == '?' || c == '#' {
				break
			}
			if c == '.' {
				dot = i
				extValid = true
			} else if dot >= 0 && !isASCIILetter(c) && !isDigit(c) {
				extValid = false
			}
			i++
		}
		if p.segmentCount == maxSegments {
			p.segmentOverflow = true
			continue
		}
		full := span{start, i}
		stem, ext := full, span{}
		if dot > start && extValid {
			if extLen := i - dot - 1; extLen >= 1 && extLen <= 8 {
				stem = span{start, dot}
				ext = span{dot + 1, i}
			}
		}
		p.segments[p.segmentCount] = segment{full: full, stem: stem, ext: ext}
		p.segmentCount++
	}

	if queryStart < end {
		parseQuery(raw, queryStart+1, end, p)
	}
	return true
}

func findScheme(raw []byte, end int) int {
	for i := 0; i+2 < end; i++ {
		switch raw[i] {
		case ':':
			if i > 0 && raw[i+1] == '/' && raw[i+2] == '/' {
				return i
			}
			return -1
		case '/', '?':
			return -1
		}
	}
	return -1
}

func parseAuthority(raw []byte, start int, p *parsedURL) int {
	i := start
	for i < len(raw) && raw[i] != '/' && raw[i] != '?' && raw[i] != '#' {
		i++
	}

	// Userinfo is not part of the host and can contain credentials. Keep an
	// explicit port so authority normalization preserves routing semantics.
	hostStart := start
	for j := start; j < i; j++ {
		if raw[j] == '@' {
			hostStart = j + 1
		}
	}
	if hostStart < i && raw[hostStart] == '[' {
		j := hostStart + 1
		for j < i && raw[j] != ']' {
			j++
		}
		if j < i {
			p.host = span{hostStart, j + 1}
			if j+1 < i && raw[j+1] == ':' {
				p.port = span{j + 2, i}
			}
			return i
		}
	}
	portStart := i
	for j := hostStart; j < i; j++ {
		if raw[j] == ':' {
			portStart = j
			break
		}
	}
	p.host = span{hostStart, portStart}
	if portStart < i {
		p.port = span{portStart + 1, i}
	}
	return i
}

func parseQuery(raw []byte, start, end int, p *parsedURL) {
	for start < end {
		partEnd := start
		for partEnd < end && raw[partEnd] != '&' {
			partEnd++
		}
		if partEnd > start {
			if p.paramCount == maxParams {
				p.paramOverflow = true
				return
			}
			eq := start
			for eq < partEnd && raw[eq] != '=' {
				eq++
			}
			q := &p.params[p.paramCount]
			q.key = span{start, eq}
			q.value = span{partEnd, partEnd}
			if eq < partEnd {
				q.hasValue = true
				q.value = span{eq + 1, partEnd}
			}
			if q.key.len() != 0 {
				p.paramCount++
			}
		}
		start = partEnd + 1
	}
}

func splitExtension(raw []byte, full span) (span, span) {
	dot := -1
	for i := full.end - 1; i > full.start; i-- {
		if raw[i] == '.' {
			dot = i
			break
		}
	}
	if dot < 0 || full.end-dot-1 > 8 {
		return full, span{}
	}
	for i := dot + 1; i < full.end; i++ {
		c := raw[i]
		if !isASCIILetter(c) && !isDigit(c) {
			return full, span{}
		}
	}
	return span{full.start, dot}, span{dot + 1, full.end}
}
