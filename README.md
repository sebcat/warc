# warc
--
    import "github.com/sebcat/warc"

Sequential WARC file reader library supporting record-at-time compression this
requires go1.4 due to its usage of compress/gzip.Reader#Multistream

Currently only works on record-at-time compressed .gz files

Example:

    code
    code

see also:

    URL to implementation repo

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
returns io.EOF when done

#### type Record

```go
type Record struct {
	Fields NamedFields
	Block  []byte
}
```
