// Sequential WARC file reader library supporting record-at-time compression
// this requires go1.4 due to its usage of compress/gzip.Reader#Multistream
//
// Currently only works on record-at-time compressed .gz files
//
// Example:
//
//     code
//     code
//
// see also:
//     URL to implementation repo
package warc

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
)

var ErrMalformedRecord = errors.New("malformed record")

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
	bc   *byteCounter
	zr   *gzip.Reader
	last int64
}

type NamedField struct {
	Name  string
	Value string
}

type Record struct {
	Fields []NamedField
	Block  []byte
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

func (r *Reader) gzipRecord() ([]byte, error) {
	var rec bytes.Buffer
	_, err := io.Copy(&rec, r.zr)
	if err != nil {
		return nil, err
	}

	if r.last == r.bc.offset {
		return nil, io.EOF
	}

	r.last = r.bc.offset
	r.zr.Reset(r.bc)
	r.zr.Multistream(false)
	return rec.Bytes(), nil
}

func (r *Reader) plainRecord() ([]byte, error) {
	// TODO: Implement
	return nil, errors.New("support for non record-at-time compressed WARC files not yet implemented")
}

func (r *Reader) record() ([]byte, error) {
	if r.zr != nil {
		return r.gzipRecord()
	} else {
		return r.plainRecord()
	}
}

// returns io.EOF when done
func (r *Reader) Next() (*Record, error) {

	rec, err := r.record()
	if err != nil {
		return nil, err
	}

	parts := bytes.SplitN(rec, []byte("\r\n\r\n"), 2)
	if len(parts) != 2 {
		return nil, ErrMalformedRecord
	}

	fmt.Printf("\"%v\" \"%v\"", string(parts[0]), string(parts[1]))

	return nil, errors.New("NYI")
}
