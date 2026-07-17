// Package stream contains shared line-oriented command I/O helpers.
package stream

import (
	"bufio"
	"io"
	"os"
)

const maxTokenSize = 4 * 1024 * 1024

// ScanLines calls consume for each input line without its trailing newline.
func ScanLines(input io.Reader, consume func([]byte) error) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), maxTokenSize)
	for scanner.Scan() {
		if err := consume(scanner.Bytes()); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// OpenOutput opens path for writing. A path of "-" returns standard without
// taking ownership of it.
func OpenOutput(path string, standard io.Writer) (io.WriteCloser, error) {
	if path == "-" {
		return nopWriteCloser{Writer: standard}, nil
	}
	return os.Create(path)
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }
