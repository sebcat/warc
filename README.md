# warc
--
    import "github.com/sebcat/warc"

Sequential WARC file reader library supporting record-at-time compression this
requires go1.4 due to its usage of compress/gzip.Reader#Multistream

Currently only works on record-at-time compressed .gz files

[Example implementation](https://github.com/sebcat/warc-urls)

## Usage

```go
var (
	ErrMalformedRecord = errors.New("malformed record")
	ErrNonWARCRecord   = errors.New("non-WARC/1.0 record")
)
```

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

#### func  NewReader

```go
func NewReader(reader io.Reader) *Reader
```

#### func (*Reader) Next

```go
func (r *Reader) Next() (*Record, error)
```
Scans and parses a WARC record from a stream. returns io.EOF when done

#### func (*Reader) NextRaw

```go
func (r *Reader) NextRaw() ([]byte, error)
```
Scans a stream for a raw WARC record. Doesn't do any validation or parsing.
Useful for concurrency pipelines where the parsing and message handling is
fanned out to multiple goroutines. See Record#FromBytes

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
func (w *Writer) WriteRecord(r *Record) error
```
Write a record. No validation of mandatory WARC fields is performed. The written
record will be an independent GZIP stream.
