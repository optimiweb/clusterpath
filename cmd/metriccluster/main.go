// Command metriccluster creates a bounded URL-cluster census for request and
// cache-miss metrics. By default it trains on a seekable input file, collects
// metric candidates with the resulting frozen model, selects a dictionary,
// then replays the input to report each cluster's totals. With -window-size it
// instead learns and rebalances online in one pass, reporting each dictionary
// version separately so reused IDs never merge unrelated totals.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/optimiweb/clusterpath"
	"github.com/optimiweb/clusterpath/cmd/internal/stream"
)

type options struct {
	input           string
	output          string
	maxClusters     int
	exactClusters   int
	maxCandidates   int
	minSamples      uint64
	cacheMissColumn int
	windowSize      uint64
}

type totals struct {
	requests uint64
	misses   uint64
}

type reportRow struct {
	cluster clusterpath.MetricCluster
	totals  totals
}

type windowReport struct {
	version  uint64
	clusters []clusterpath.MetricCluster
	counts   map[int]totals
}

func main() {
	var opts options
	flag.StringVar(&opts.input, "in", "urls.txt", "input URL file; one URL per line, or tab-separated records")
	flag.StringVar(&opts.output, "out", "metric_clusters.tsv", "output TSV file, or - for stdout")
	flag.IntVar(&opts.maxClusters, "max-clusters", 96, "maximum metric clusters, including fixed fallbacks")
	flag.IntVar(&opts.exactClusters, "exact-clusters", 0, "dynamic slots reserved for exact route templates (0 = default)")
	flag.IntVar(&opts.maxCandidates, "max-candidates", 0, "retained route and family candidates (0 = default)")
	flag.Uint64Var(&opts.minSamples, "min-samples", 10, "requests required before a dynamic cluster is eligible")
	flag.IntVar(&opts.cacheMissColumn, "cache-miss-column", -1, "zero-based tab-separated cache-miss column (-1 = all hits)")
	flag.Uint64Var(&opts.windowSize, "window-size", 0, "online requests per dictionary window (0 = batch mode)")
	flag.Parse()

	if err := run(opts); err != nil {
		fmt.Fprintln(os.Stderr, "metriccluster:", err)
		os.Exit(1)
	}
}

func run(opts options) (err error) {
	if opts.windowSize > 0 {
		return runWindowed(opts)
	}
	if opts.input == "-" {
		return fmt.Errorf("-in must be a seekable file because metriccluster uses two passes")
	}
	if opts.cacheMissColumn < -1 {
		return fmt.Errorf("-cache-miss-column must be -1 or greater")
	}
	input, err := os.Open(opts.input)
	if err != nil {
		return err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("-in must be a regular seekable file")
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

	return runPipeline(opts, input, output)
}

func runWindowed(opts options) (err error) {
	var input io.Reader = os.Stdin
	var inputFile *os.File
	if opts.input != "-" {
		inputFile, err = os.Open(opts.input)
		if err != nil {
			return err
		}
		defer inputFile.Close()
		input = inputFile
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
	return runWindowedPipeline(opts, input, output)
}

func runPipeline(opts options, input io.ReadSeeker, output io.Writer) error {
	m := clusterpath.NewMetricClusterer(clusterpath.MetricConfig{
		MaxClusters:   opts.maxClusters,
		ExactClusters: opts.exactClusters,
		MaxCandidates: opts.maxCandidates,
		MinSamples:    opts.minSamples,
	})
	if err := stream.ScanLines(input, func(line []byte) error {
		url, _, err := parseRecord(line, opts.cacheMissColumn)
		if err != nil {
			return err
		}
		m.Train(url)
		return nil
	}); err != nil {
		return fmt.Errorf("train normalizer: %w", err)
	}
	m.FreezeNormalizer()
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind input after training: %w", err)
	}

	if err := stream.ScanLines(input, func(line []byte) error {
		url, miss, err := parseRecord(line, opts.cacheMissColumn)
		if err != nil {
			return err
		}
		m.Observe(url, miss)
		return nil
	}); err != nil {
		return fmt.Errorf("collect candidates: %w", err)
	}
	m.Rebalance()
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind input: %w", err)
	}

	counts := make(map[int]totals)
	if err := stream.ScanLines(input, func(line []byte) error {
		url, miss, err := parseRecord(line, opts.cacheMissColumn)
		if err != nil {
			return err
		}
		cluster := m.Assign(url)
		total := counts[cluster.ID]
		total.requests++
		if miss {
			total.misses++
		}
		counts[cluster.ID] = total
		return nil
	}); err != nil {
		return fmt.Errorf("replay: %w", err)
	}

	if err := writeReport(output, m.Clusters(), counts); err != nil {
		return err
	}
	return nil
}

func runWindowedPipeline(opts options, input io.Reader, output io.Writer) error {
	m := newMetricClusterer(opts)
	reports := make([]windowReport, 0)
	counts := make(map[int]totals)
	clusters := m.Clusters()
	var windowRequests uint64
	var version uint64 = 1

	flush := func() {
		if windowRequests == 0 {
			return
		}
		reports = append(reports, windowReport{version: version, clusters: clusters, counts: counts})
		m.Rebalance()
		m.ResetWindow()
		version++
		clusters = m.Clusters()
		counts = make(map[int]totals)
		windowRequests = 0
	}

	if err := stream.ScanLines(input, func(line []byte) error {
		url, miss, err := parseRecord(line, opts.cacheMissColumn)
		if err != nil {
			return err
		}
		cluster := m.Observe(url, miss)
		total := counts[cluster.ID]
		total.requests++
		if miss {
			total.misses++
		}
		counts[cluster.ID] = total
		windowRequests++
		if windowRequests == opts.windowSize {
			flush()
		}
		return nil
	}); err != nil {
		return fmt.Errorf("stream: %w", err)
	}
	flush()
	return writeWindowedReport(output, reports)
}

func newMetricClusterer(opts options) *clusterpath.MetricClusterer {
	return clusterpath.NewMetricClusterer(clusterpath.MetricConfig{
		MaxClusters:   opts.maxClusters,
		ExactClusters: opts.exactClusters,
		MaxCandidates: opts.maxCandidates,
		MinSamples:    opts.minSamples,
	})
}

func parseRecord(line []byte, cacheMissColumn int) ([]byte, bool, error) {
	if cacheMissColumn < 0 {
		return line, false, nil
	}
	fields := bytes.Split(line, []byte{'\t'})
	if len(fields) <= cacheMissColumn {
		return nil, false, fmt.Errorf("cache-miss column %d missing", cacheMissColumn)
	}
	miss, err := parseCacheMiss(fields[cacheMissColumn])
	if err != nil {
		return nil, false, err
	}
	return fields[0], miss, nil
}

func parseCacheMiss(value []byte) (bool, error) {
	switch strings.ToLower(string(value)) {
	case "0", "false", "hit":
		return false, nil
	case "1", "true", "miss":
		return true, nil
	default:
		return false, fmt.Errorf("invalid cache-miss value %q", value)
	}
}

func writeReport(output io.Writer, clusters []clusterpath.MetricCluster, counts map[int]totals) error {
	rows := make([]reportRow, 0, len(clusters))
	for _, cluster := range clusters {
		rows = append(rows, reportRow{cluster: cluster, totals: counts[cluster.ID]})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].totals.requests == rows[j].totals.requests {
			return rows[i].cluster.ID < rows[j].cluster.ID
		}
		return rows[i].totals.requests > rows[j].totals.requests
	})

	writer := bufio.NewWriterSize(output, 256*1024)
	if _, err := fmt.Fprintln(writer, "cluster_id\tkind\trequests\tcache_misses\tcache_miss_rate\tlabel"); err != nil {
		return err
	}
	var requests, misses uint64
	for _, row := range rows {
		requests += row.totals.requests
		misses += row.totals.misses
		rate := 0.0
		if row.totals.requests != 0 {
			rate = float64(row.totals.misses) / float64(row.totals.requests)
		}
		if _, err := fmt.Fprintf(writer, "%d\t%s\t%d\t%d\t%.6f\t%s\n",
			row.cluster.ID, kindName(row.cluster.Kind), row.totals.requests, row.totals.misses, rate, row.cluster.Label); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(writer, "# requests=%d cache_misses=%d clusters=%d\n", requests, misses, len(rows)); err != nil {
		return err
	}
	return writer.Flush()
}

func writeWindowedReport(output io.Writer, reports []windowReport) error {
	writer := bufio.NewWriterSize(output, 256*1024)
	if _, err := fmt.Fprintln(writer, "dictionary_version\tcluster_id\tkind\thits\tcache_misses\tcache_miss_rate\tlabel"); err != nil {
		return err
	}
	var requests, misses uint64
	for _, report := range reports {
		rows := make([]reportRow, 0, len(report.clusters))
		for _, cluster := range report.clusters {
			rows = append(rows, reportRow{cluster: cluster, totals: report.counts[cluster.ID]})
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].totals.requests == rows[j].totals.requests {
				return rows[i].cluster.ID < rows[j].cluster.ID
			}
			return rows[i].totals.requests > rows[j].totals.requests
		})
		for _, row := range rows {
			requests += row.totals.requests
			misses += row.totals.misses
			rate := 0.0
			if row.totals.requests != 0 {
				rate = float64(row.totals.misses) / float64(row.totals.requests)
			}
			if _, err := fmt.Fprintf(writer, "%d\t%d\t%s\t%d\t%d\t%.6f\t%s\n",
				report.version, row.cluster.ID, kindName(row.cluster.Kind), row.totals.requests, row.totals.misses, rate, row.cluster.Label); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintf(writer, "# requests=%d cache_misses=%d dictionary_versions=%d\n", requests, misses, len(reports)); err != nil {
		return err
	}
	return writer.Flush()
}

func kindName(kind clusterpath.MetricClusterKind) string {
	switch kind {
	case clusterpath.MetricRouteCluster:
		return "route"
	case clusterpath.MetricFamilyCluster:
		return "family"
	default:
		return "fallback"
	}
}
