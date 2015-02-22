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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
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

func NewReader(reader io.Reader) *Reader {
	return &Reader{
		bc: &byteCounter{
			r: bufio.NewReader(reader),
		},
	}
}

func NewGZIPReader(reader io.Reader) (r *Reader, err error) {
	r = NewReader(reader)
	r.zr, err = gzip.NewReader(r.bc)
	if err != nil {
		return nil, err
	}

	r.zr.Multistream(false)
	return r, nil
}

type record []byte

func (r *Reader) gzipRecord() (record, error) {
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

func (r *Reader) plainRecord() (record, error) {
	// TODO: Implement
	return nil, errors.New("NYI")
}

func (r *Reader) record() (rec record, err error) {
	if r.zr != nil {
		rec, err = r.gzipRecord()
	} else {
		rec, err = r.plainRecord()
	}

	return
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("specify path to WARC file")
		os.Exit(1)
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer f.Close()

	var x *Reader
	if strings.HasSuffix(os.Args[1], ".warc.gz") {
		x, err = NewGZIPReader(f)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	} else {
		x = NewReader(f)
	}

	for {
		rec, err := x.record()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("record error: ", err)
			break
		}

		fmt.Println("RECORD\n------\n\n", string(rec))
	}
}
