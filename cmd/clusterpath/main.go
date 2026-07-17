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
	"github.com/optimiweb/clusterpath/cmd/internal/stream"
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
	defaults := clusterpath.DefaultConfig()
	flag.StringVar(&opts.input, "in", "-", "input path file, or - for stdin")
	flag.StringVar(&opts.output, "out", "-", "normalized output file, or - for stdout")
	flag.StringVar(&opts.report, "report", "", "optional template count report file, or - for stderr")
	flag.BoolVar(&opts.twoPass, "two-pass", true, "learn once, freeze, then emit stable templates")
	flag.IntVar(&opts.maxBuckets, "max-buckets", defaults.MaxBuckets, "maximum in-memory structural buckets")
	flag.UintVar(&opts.minSamples, "min-samples", uint(defaults.MinSamples), "samples required before learned decisions")
	flag.UintVar(&opts.distinctLimit, "distinct-limit", uint(defaults.DistinctLimit), "absolute distinct-value threshold")
	flag.Float64Var(&opts.highCardRatio, "high-card-ratio", defaults.HighCardRatio, "distinct/total ratio above which a position is masked")
	flag.IntVar(&opts.signaturePrefix, "signature-prefix", defaults.SignaturePrefix, "leading literal segments folded into the bucket key (-1 = group by shape only)")
	flag.Parse()

	if err := run(opts); err != nil {
		fmt.Fprintln(os.Stderr, "clusterpath:", err)
		os.Exit(1)
	}
}

type lineSource func(func([]byte) error) error

func run(opts options) (err error) {
	if opts.twoPass && opts.input == "-" {
		return fmt.Errorf("-two-pass requires a seekable -in file; use -two-pass=false for stdin")
	}

	source := fileSource(opts.input)
	if opts.input == "-" {
		source = func(consume func([]byte) error) error {
			return stream.ScanLines(os.Stdin, consume)
		}
	}
	output, err := stream.OpenOutput(opts.output, os.Stdout)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := output.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	var report io.WriteCloser
	if opts.report != "" {
		report, err = stream.OpenOutput(opts.report, os.Stderr)
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := report.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}()
	}

	return runPipeline(opts, source, output, report)
}

func runPipeline(opts options, source lineSource, output io.Writer, report io.Writer) error {
	cfg, err := configFromOptions(opts)
	if err != nil {
		return err
	}
	c := clusterpath.New(cfg)

	if opts.twoPass {
		learnDst := make([]byte, 0, 1024)
		if err := source(func(line []byte) error {
			learnDst = c.Normalize(learnDst[:0], line)
			return nil
		}); err != nil {
			return fmt.Errorf("learn: %w", err)
		}
		c.Freeze()
	}

	buffered := bufio.NewWriterSize(output, 256*1024)

	var counts map[string]uint64
	if report != nil {
		counts = make(map[string]uint64)
	}
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
		if report != nil {
			counts[string(dst)]++
		}
		emitted++
		return nil
	}
	if err := source(process); err != nil {
		return err
	}
	if err := buffered.Flush(); err != nil {
		return err
	}
	if report != nil {
		if err := writeReport(report, counts, emitted, c.Stats()); err != nil {
			return err
		}
	}
	return nil
}

func configFromOptions(opts options) (clusterpath.Config, error) {
	if uint64(opts.minSamples) > uint64(^uint32(0)) {
		return clusterpath.Config{}, fmt.Errorf("-min-samples must be at most %d", ^uint32(0))
	}
	if uint64(opts.distinctLimit) > uint64(^uint16(0)) {
		return clusterpath.Config{}, fmt.Errorf("-distinct-limit must be at most %d", ^uint16(0))
	}
	return clusterpath.Config{
		MaxBuckets:      opts.maxBuckets,
		MinSamples:      uint32(opts.minSamples),
		DistinctLimit:   uint16(opts.distinctLimit),
		HighCardRatio:   opts.highCardRatio,
		SignaturePrefix: opts.signaturePrefix,
	}, nil
}

func fileSource(path string) lineSource {
	return func(consume func([]byte) error) error {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		return stream.ScanLines(file, consume)
	}
}

type reportRow struct {
	template string
	count    uint64
}

func writeReport(writer io.Writer, counts map[string]uint64, emitted uint64, stats clusterpath.Stats) error {
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
