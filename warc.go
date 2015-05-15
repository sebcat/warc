// WARC package supporting record-at-time compression.
// Requires go1.4 due to its usage of compress/gzip.Reader#Multistream
//
// Currently only works on record-at-time compressed .gz files.
//
// Supports indexed operations for concurrent reading. One use-case
// is to have one goroutine passing Offset's to a channel that
// multiple goroutines read from. Each of these goroutines have
// their own WARC Reader and can read and decompress a record
// independantly.
//
// [Example implementation](https://github.com/sebcat/warc-urls)
package warc

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"strings"
)

var (
	ErrMalformedRecord = errors.New("malformed record")
	ErrNonWARCRecord   = errors.New("non-WARC/1.0 record")
	ErrOffsetOverflow  = errors.New("offset overflow")
	ErrNotASeeker      = errors.New("the underlying stream is not seekable")
)

// a combo of a buffered reader and an offset counter.
// we're doing this because the alternative approach of
// wrapping bufio steals a bit of time since the reader
// will be called in a tight loop when used as an
// io.ByteReader inside
// compress/flate.(*decompressor).moreBits (go1.4.2)
type reader struct {
	r io.Reader

	rbuf     [4096]byte
	nbufleft int
	rpos     int

	// number of bytes having been read from this reader
	// (i.e., not from r)
	nread int64
}

func (r *reader) next() (n int, err error) {
	n, err = r.r.Read(r.rbuf[:])
	r.nbufleft = n
	r.rpos = 0
	return
}

func (r *reader) Read(p []byte) (n int, err error) {
	ncopy := len(p)
	if r.nbufleft == 0 {
		ncopy, err = r.next()
	}

	if ncopy > r.nbufleft {
		ncopy = r.nbufleft
	}

	n = copy(p, r.rbuf[r.rpos:r.rpos+r.nbufleft])
	r.rpos += n
	r.nread += int64(n)
	r.nbufleft -= n
	return
}

// This is a bit of a bottle neck, as it's called often
func (r *reader) ReadByte() (c byte, err error) {
	if r.nbufleft == 0 {
		if _, err = r.next(); r.nbufleft == 0 {
			return 0, err
		}
	}

	c = r.rbuf[r.rpos]
	r.nread++
	r.rpos++
	r.nbufleft--
	return
}

type Offset int64

type Reader struct {
	r    *reader
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
	// TODO: Maybe use net/textproto for all of this in here?
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

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r: &reader{
			r: r,
		},
	}
}

func NewGZIPReader(reader io.Reader) (r *Reader, err error) {
	r = NewReader(reader)
	r.zr, err = gzip.NewReader(r.r)
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

	if r.last == r.r.nread {
		return nil, io.EOF
	}

	r.last = r.r.nread
	r.zr.Reset(r.r)
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

// Scans a raw WARC record from a stream at a specific offset
// from the start of the stream.
// The original Reader passed to NewReader must implement the
// io.Seeker interface. The Reader stream will be at the
// position after the read record on successful return.
func (r *Reader) NextRawAt(offset Offset) ([]byte, error) {
	if seeker, ok := r.r.r.(io.Seeker); ok {
		if _, err := seeker.Seek(int64(offset), 0); err != nil {
			return nil, err
		}
	} else {
		return nil, ErrNotASeeker
	}

	return r.record()
}

// Scans and parses a WARC record from a stream at a specific offset
// from the start of the stream.
// The original Reader passed to NewReader must implement the
// io.Seeker interface. The Reader stream will be at the
// position after the read record on successful return.
func (r *Reader) NextAt(offset Offset) (*Record, error) {
	if seeker, ok := r.r.r.(io.Seeker); ok {
		if _, err := seeker.Seek(int64(offset), 0); err != nil {
			return nil, err
		}
	} else {
		return nil, ErrNotASeeker
	}

	return r.Next()
}

type cwriter struct {
	c Offset
	w io.Writer
}

func newCWriter(w io.Writer) *cwriter {
	return &cwriter{w: w}
}

func (w *cwriter) Write(p []byte) (n int, err error) {
	before := w.c
	n, err = w.w.Write(p)
	w.c += Offset(n)
	if w.c < before {
		err = ErrOffsetOverflow
	}

	return
}

func (w *cwriter) Offset() Offset {
	return w.c
}

type Writer struct {
	cw    *cwriter
	zw    *gzip.Writer
	index io.Writer
}

func NewIndexingWriter(w io.Writer, index io.Writer) *Writer {
	cw := newCWriter(w)
	zw := gzip.NewWriter(cw)
	return &Writer{cw: cw, zw: zw, index: index}
}

func NewWriter(w io.Writer) *Writer {
	return NewIndexingWriter(w, nil)
}

// Write a record. No validation of mandatory WARC fields is performed.
// The written record will be an independent GZIP stream.
func (w *Writer) WriteRecord(r *Record) error {
	rec := r.Bytes()
	offset := w.cw.Offset()
	_, err := w.zw.Write(rec)
	if err != nil {
		return err
	}

	if err := w.zw.Close(); err != nil {
		return err
	}

	w.zw.Reset(w.cw)
	if w.index != nil {
		if err := binary.Write(w.index, binary.LittleEndian, &offset); err != nil {
			return err
		}
	}

	return nil
}

// Read an Offset from a WARC index
func ReadOffset(r io.Reader) (Offset, error) {
	var offset Offset
	err := binary.Read(r, binary.LittleEndian, &offset)
	return offset, err
}
