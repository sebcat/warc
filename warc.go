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
	"io"
	"strconv"
	"strings"
)

var (
	ErrMalformedRecord = errors.New("malformed record")
	ErrNonWARCRecord   = errors.New("non-WARC/1.0 record")
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
	bc   *byteCounter
	zr   *gzip.Reader
	last int64
}

type NamedField struct {
	Name  string
	Value string
}

type NamedFields []NamedField

type Record struct {
	Fields NamedFields
	Block  []byte
}

func (r *Record) Bytes() []byte {
	var b bytes.Buffer
	b.Write([]byte("WARC/1.0\r\n"))
	for _, field := range r.Fields {
		b.Write([]byte(field.Name + ": " + field.Value + "\r\n"))
	}

	b.Write([]byte("\r\n"))
	b.Write([]byte(r.Block))
	b.Write([]byte("\r\n\r\n"))
	return b.Bytes()
}

func (r *Record) FromBytes(rec []byte) error {
	parts := bytes.SplitN(rec, []byte("\r\n\r\n"), 2)
	if len(parts) != 2 {
		return ErrMalformedRecord
	}

	hdr, block := parts[0], parts[1]
	r.Block = block
	for ix, hdrline := range bytes.Split(hdr, []byte("\r\n")) {
		if ix == 0 {
			if string(hdrline) != "WARC/1.0" {
				return ErrNonWARCRecord
			}

			continue
		}

		l := bytes.SplitN(hdrline, []byte(":"), 2)
		if len(l) != 2 {
			return ErrMalformedRecord
		}

		r.Fields = append(r.Fields, NamedField{
			Name:  string(bytes.Trim(l[0], "\r\n\t ")),
			Value: string(bytes.Trim(l[1], "\r\n\t ")),
		})
	}

	lenStr := r.Fields.Value("content-length")
	if i, err := strconv.Atoi(lenStr); err == nil {
		r.Block = r.Block[:i]
	}

	return nil
}

func (f NamedFields) Value(name string) string {
	for _, el := range f {
		if strings.EqualFold(el.Name, name) {
			return el.Value
		}
	}

	return ""
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

// Scans a stream for a raw WARC record. Doesn't do any
// validation or parsing. Useful for concurrency pipelines
// where the parsing and message handling is fanned out to
// multiple goroutines. See Record#FromBytes
func (r *Reader) NextRaw() ([]byte, error) {
	return r.record()
}

// Scans and parses a WARC record from a stream.
// returns io.EOF when done
func (r *Reader) Next() (*Record, error) {
	var res Record

	rec, err := r.record()
	if err != nil {
		return nil, err
	}

	if err := res.FromBytes(rec); err != nil {
		return nil, err
	}

	return &res, nil
}

type Writer struct {
	w  io.Writer
	zw *gzip.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w, zw: gzip.NewWriter(w)}
}

// Write a record. No validation of mandatory WARC fields is performed.
// The written record will be an independent GZIP stream.
func (w *Writer) WriteRecord(r *Record) error {
	rec := r.Bytes()
	if _, err := w.zw.Write(rec); err != nil {
		return err
	}

	err := w.zw.Close()
	w.zw.Reset(w.w)
	return err
}
