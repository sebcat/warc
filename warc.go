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
	"io/ioutil"
	"os"
)

type byteCounter struct {
	r      *bufio.Reader
	offset int64
}

func (bc *byteCounter) Read(p []byte) (n int, err error) {
	n, err = bc.r.Read(p)
	bc.offset += int64(n)
	return
}

func (bc *byteCounter) ReadByte() (c byte, err error) {
	c, err = bc.r.ReadByte()
	if err == nil {
		bc.offset++
	}

	return
}

type Reader struct {
	f    *os.File
	bc   *byteCounter
	zr   *gzip.Reader
	last int64
}

func Open(archive string) (*Reader, error) {
	f, err := os.Open(archive)
	if err != nil {
		return nil, err
	}

	r := &Reader{f: f,
		bc: &byteCounter{r: bufio.NewReader(f)},
	}

	r.zr, err = gzip.NewReader(r.bc)
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

func (r *Reader) record() (record, error) {
	b, err := ioutil.ReadAll(r.zr)
	if err != nil {
		return nil, err
	}

	if r.last == r.bc.offset {
		return nil, io.EOF
	}

	r.last = r.bc.offset
	r.zr.Reset(r.bc)
	r.zr.Multistream(false)

	return record(b), nil
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
