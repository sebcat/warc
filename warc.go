// Sequential WARC file reader library supporting record-at-time compression
// this requires go1.4 due to its usage of compress/gzip.Reader#Multistream
//
// Example:
//
//     code
//     code
//
// see also:
//     URL to implementation repo
package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
)

type Reader struct {
	f  *os.File
	b  *bufio.Reader
	zr *gzip.Reader
}

func Open(archive string) (*Reader, error) {
	f, err := os.Open(archive)
	if err != nil {
		return nil, err
	}

	r := &Reader{f: f,
		b: bufio.NewReader(f),
	}

	r.zr, err = gzip.NewReader(r.b)
	if err != nil {
		return nil, err
	}

	r.zr.Multistream(false)
	return r, nil
}

func (r *Reader) Close() error {
	if r != nil && r.f != nil {
		return r.f.Close()
	}

	return nil
}

type record []byte

func (r *Reader) record() (rec record, err error) {
	data := make([]byte, 1024)
	_, err = r.zr.Read(data)
	if err == io.EOF {
		r.zr.Reset(r.b)
		r.zr.Multistream(false)
		_, err = r.zr.Read(data)
	}
	rec = record(data)
	return
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("specify path to WARC file")
		os.Exit(1)
	}

	x, err := Open(os.Args[1])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer x.Close()
	for {
		rec, err := x.record()
		if err != nil {
			fmt.Println("record error: ", err)
			break
		}

		fmt.Println("RECORD\n------\n\n", string(rec))
	}
}
