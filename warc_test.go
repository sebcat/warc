package warc

import (
	"bytes"
	"io"
	"testing"
)

func TestWriteReadGZIP(t *testing.T) {
	var b bytes.Buffer
	rec := &Record{
		Fields: []NamedField{
			{"WARC-Type", "foo"},
			{"WARC-Date", "today"},
			{"WARC-Record-ID", "urn:2"},
			{"Content-Length", "3"},
		},
		Block: []byte("lel"),
	}

	w := NewWriter(&b)
	if _, err := w.WriteRecord(rec); err != nil {
		t.Fatal(err)
	}

	r, err := NewGZIPReader(&b)
	if err != nil {
		t.Fatal(err)
	}

	readRec, err := r.Next()
	if err != nil {
		t.Fatal(readRec, err)
	}

	_, err = r.Next()
	if err != io.EOF {
		t.Fatal("expected io.EOF, got", err)
	}

	if !bytes.Equal(rec.Block, readRec.Block) {
		t.Fatal(rec.Block, " != ", readRec.Block)
	}

	for i, _ := range rec.Fields {
		if rec.Fields[i].Name != readRec.Fields[i].Name {
			t.Fatal(rec.Fields[i].Name, " != ", readRec.Fields[i].Name)
		}

		if rec.Fields[i].Value != readRec.Fields[i].Value {
			t.Fatal(rec.Fields[i].Value, " != ", readRec.Fields[i].Value)
		}
	}
}
