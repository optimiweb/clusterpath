// Command clusterpath normalizes a stream of URLs into clustered templates to
// reduce cardinality before analytics ingestion.
//
// By default it runs two passes over a seekable input file: the first learns
// the model, then it freezes and emits a stable normalized line per input.
// With -report it also writes a template/count census.
//
// Usage:
//
//	clusterpath -in urls.txt -out normalized.txt -report clusters.tsv
//	cat urls.txt | clusterpath -two-pass=false          # streaming, single pass
//
// Flags -signature-prefix, -min-samples, -distinct-limit, and -high-card-ratio
// map onto the corresponding clusterpath.Config fields; -signature-prefix -1
// groups purely by structural shape (clusterpath.GroupByShape).
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/optimiweb/clusterpath"
)

type options struct {
	input           string
	output          string
	report          string
	twoPass         bool
	maxBuckets      int
	minSamples      uint
	distinctLimit   uint
	highCardRatio   float64
	signaturePrefix int
}

func main() {
	var opts options
	flag.StringVar(&opts.input, "in", "-", "input path file, or - for stdin")
	flag.StringVar(&opts.output, "out", "-", "normalized output file, or - for stdout")
	flag.StringVar(&opts.report, "report", "", "optional template count report file, or - for stderr")
	flag.BoolVar(&opts.twoPass, "two-pass", true, "learn once, freeze, then emit stable templates")
	flag.IntVar(&opts.maxBuckets, "max-buckets", 4096, "maximum in-memory structural buckets")
	flag.UintVar(&opts.minSamples, "min-samples", 32, "samples required before learned decisions")
	flag.UintVar(&opts.distinctLimit, "distinct-limit", 64, "absolute distinct-value threshold")
	flag.Float64Var(&opts.highCardRatio, "high-card-ratio", 0.5, "distinct/total ratio above which a position is masked")
	flag.IntVar(&opts.signaturePrefix, "signature-prefix", 1, "leading literal segments folded into the bucket key (-1 = group by shape only)")
	flag.Parse()

	if err := run(opts); err != nil {
		fmt.Fprintln(os.Stderr, "clusterpath:", err)
		os.Exit(1)
	}
}

func run(opts options) error {
	if opts.twoPass && opts.input == "-" {
		return fmt.Errorf("-two-pass requires a seekable -in file; use -two-pass=false for stdin")
	}
	c := clusterpath.New(clusterpath.Config{
		MaxBuckets:      opts.maxBuckets,
		MinSamples:      uint32(opts.minSamples),
		DistinctLimit:   uint16(opts.distinctLimit),
		HighCardRatio:   opts.highCardRatio,
		SignaturePrefix: opts.signaturePrefix,
	})

	if opts.twoPass {
		if err := scanFile(opts.input, func(line []byte) error {
			c.Normalize(nil, line)
			return nil
		}); err != nil {
			return fmt.Errorf("learn: %w", err)
		}
		c.Freeze()
	}

	output, closeOutput, err := openOutput(opts.output)
	if err != nil {
		return err
	}
	defer closeOutput()
	buffered := bufio.NewWriterSize(output, 256*1024)
	defer buffered.Flush()

	counts := make(map[string]uint64)
	var emitted uint64
	dst := make([]byte, 0, 1024)
	process := func(line []byte) error {
		if opts.twoPass {
			dst = c.Apply(dst[:0], line)
		} else {
			dst = c.Normalize(dst[:0], line)
		}
		if _, err := buffered.Write(dst); err != nil {
			return err
		}
		if err := buffered.WriteByte('\n'); err != nil {
			return err
		}
		if opts.report != "" {
			counts[string(dst)]++
		}
		emitted++
		return nil
	}
	if opts.input == "-" {
		err = scanReader(os.Stdin, process)
	} else {
		err = scanFile(opts.input, process)
	}
	if err != nil {
		return err
	}
	if err := buffered.Flush(); err != nil {
		return err
	}
	if opts.report != "" {
		if err := writeReport(opts.report, counts, emitted, c.Stats()); err != nil {
			return err
		}
	}
	return nil
}

func scanFile(path string, consume func([]byte) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return scanReader(file, consume)
}

func scanReader(reader io.Reader, consume func([]byte) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		if err := consume(scanner.Bytes()); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func openOutput(path string) (io.Writer, func(), error) {
	if path == "-" {
		return os.Stdout, func() {}, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, func() {}, err
	}
	return file, func() { _ = file.Close() }, nil
}

type reportRow struct {
	template string
	count    uint64
}

func writeReport(path string, counts map[string]uint64, emitted uint64, stats clusterpath.Stats) error {
	rows := make([]reportRow, 0, len(counts))
	for template, count := range counts {
		rows = append(rows, reportRow{template: template, count: count})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].count == rows[j].count {
			return rows[i].template < rows[j].template
		}
		return rows[i].count > rows[j].count
	})

	var writer io.Writer = os.Stderr
	var file *os.File
	var err error
	if path != "-" {
		file, err = os.Create(path)
		if err != nil {
			return err
		}
		defer file.Close()
		writer = file
	}
	buffered := bufio.NewWriter(writer)
	for _, row := range rows {
		if _, err := fmt.Fprintf(buffered, "%d\t%s\n", row.count, row.template); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(buffered, "# paths=%d clusters=%d buckets=%d evictions=%d\n",
		emitted, len(rows), stats.Buckets, stats.Evictions); err != nil {
		return err
	}
	return buffered.Flush()
}
