# warc
--
    import "github.com/sebcat/warc"

WARC package supporting record-at-time compression. Requires go1.4 due to its
usage of compress/gzip.Reader#Multistream

Currently only works on record-at-time compressed .gz files.

Supports indexed operations for concurrent reading. One use-case is to have one
goroutine passing Offset's to a channel that multiple goroutines read from. Each
of these goroutines have their own WARC Reader and can read and decompress a
record independantly.

[Example implementation](https://github.com/sebcat/warc-urls)

## Usage

```go
var (
	ErrMalformedRecord = errors.New("malformed record")
	ErrNonWARCRecord   = errors.New("non-WARC/1.0 record")
	ErrOffsetOverflow  = errors.New("offset overflow")
	ErrNotASeeker      = errors.New("the underlying stream is not seekable")
	ErrAlreadyExists   = errors.New("Record already exists")
	ErrNoSuchEntry     = errors.New("No such entry")
)
```

#### type Index

```go
type Index struct {
}
```


#### func  NewIndex

```go
func NewIndex(path string) (*Index, error)
```
Create a new file-backed index

#### func (*Index) Close

```go
func (index *Index) Close() error
```

#### func (*Index) Offset

```go
func (index *Index) Offset(id string) (int64, error)
```
Get an offset from an index by record ID

#### func (*Index) Put

```go
func (index *Index) Put(id string, offset int64) error
```
Put a new entry in the index

#### type NamedField

```go
type NamedField struct {
	Name  string
	Value string
}
```


#### type NamedFields

```go
type NamedFields []NamedField
```


#### func (NamedFields) Value

```go
func (f NamedFields) Value(name string) string
```

#### type Reader

```go
type Reader struct {
}
```


#### func  NewGZIPReader

```go
func NewGZIPReader(reader io.Reader) (r *Reader, err error)
```
Create a new record-at-time warc.gz reader

#### func  NewReader

```go
func NewReader(r io.Reader) *Reader
```

#### func (*Reader) Next

```go
func (r *Reader) Next() (*Record, error)
```
Scans and parses a WARC record from a stream. returns io.EOF when done

#### func (*Reader) NextAt

```go
func (r *Reader) NextAt(offset int64) (*Record, error)
```
Scans and parses a WARC record from a stream at a specific offset from the start
of the stream. The original Reader passed to NewReader must implement the
io.Seeker interface. The Reader stream will be at the position after the read
record on successful return.

#### func (*Reader) NextRaw

```go
func (r *Reader) NextRaw() ([]byte, error)
```
Scans a stream for a raw WARC record. Doesn't do any validation or parsing.
Useful for concurrency pipelines where the parsing and message handling is
fanned out to multiple goroutines. See Record#FromBytes

#### func (*Reader) NextRawAt

```go
func (r *Reader) NextRawAt(offset int64) ([]byte, error)
```
Scans a raw WARC record from a stream at a specific offset from the start of the
stream. The original Reader passed to NewReader must implement the io.Seeker
interface. The Reader stream will be at the position after the read record on
successful return.

#### func (*Reader) Offset

```go
func (r *Reader) Offset() int64
```
Return the current reader offset

#### type Record

```go
type Record struct {
	Fields NamedFields
	Block  []byte
}
```


#### func (*Record) Bytes

```go
func (r *Record) Bytes() []byte
```

#### func (*Record) FromBytes

```go
func (r *Record) FromBytes(rec []byte) error
```

#### type Writer

```go
type Writer struct {
}
```


#### func  NewWriter

```go
func NewWriter(w io.Writer) *Writer
```

#### func (*Writer) WriteRecord

```go
func (w *Writer) WriteRecord(r *Record) (int64, error)
```
Write a record. No validation of mandatory WARC fields is performed. The written
record will be an independent GZIP stream.

Returns the offset of the WARC record relative to the start of the stream passed
to NewWriter, or an error on failure.
