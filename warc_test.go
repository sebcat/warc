package warc

import (
	"bytes"
	"io"
	"os"
	"testing"
)

var TestRecord = &Record{
	Fields: []NamedField{
		{"WARC-Type", "foo"},
		{"WARC-Date", "today"},
		{"WARC-Record-ID", "urn:1"},
		{"Content-Length", "3"},
	},
	Block: []byte("lel"),
}

var TestRecord2 = &Record{
	Fields: []NamedField{
		{"WARC-Type", "foo"},
		{"WARC-Date", "today"},
		{"WARC-Record-ID", "urn:2"},
		{"Content-Length", "5"},
	},
	Block: []byte("tadam"),
}

func testRecordEquality(t *testing.T, r1, r2 *Record) {
	if !bytes.Equal(r1.Block, r2.Block) {
		t.Fatal(r1.Block, " != ", r2.Block)
	}

	for i, _ := range r1.Fields {
		if r1.Fields[i].Name != r2.Fields[i].Name {
			t.Fatal(r1.Fields[i].Name, " != ", r2.Fields[i].Name)
		}

		if r1.Fields[i].Value != r2.Fields[i].Value {
			t.Fatal(r1.Fields[i].Value, " != ", r2.Fields[i].Value)
		}
	}
}

func TestWriteReadGZIP(t *testing.T) {
	var b bytes.Buffer

	w := NewWriter(&b)
	if _, err := w.WriteRecord(TestRecord); err != nil {
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

	testRecordEquality(t, TestRecord, readRec)
}

func TestIndex(t *testing.T) {
	const TestDataWARC = "testdata/test.warc.gz"
	const TestDataIndex = "testdata/test.index"

	f, err := os.Create(TestDataWARC)
	if err != nil {
		t.Fatal(err)
	}

	w := NewWriter(f)
	ix, err := NewIndex(TestDataIndex)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		os.Remove(TestDataWARC)
		os.Remove(TestDataIndex)
	}()

	off1, err := w.WriteRecord(TestRecord)
	if err != nil {
		t.Fatal(err)
	}

	off2, err := w.WriteRecord(TestRecord2)
	if err != nil {
		t.Fatal(err)
	}

	if err := ix.Put("1", off1); err != nil {
		t.Fatal(err)
	}

	if err := ix.Put("2", off2); err != nil {
		t.Fatal(err)
	}

	f.Close()
	ix.Close()

	f, err = os.Open(TestDataWARC)
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewGZIPReader(f)
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()
	ix, err = NewIndex(TestDataIndex)
	if err != nil {
		t.Fatal(err)
	}

	defer ix.Close()

	if off1, err = ix.Offset("1"); err != nil {
		t.Fatal(err)
	}

	if off2, err = ix.Offset("2"); err != nil {
		t.Fatal(err)
	}

	n1, err := r.NextAt(off1)
	if err != nil {
		t.Fatal(err)
	}

	n2, err := r.NextAt(off2)
	if err != nil {
		t.Fatal(err)
	}

	testRecordEquality(t, n1, TestRecord)
	testRecordEquality(t, n2, TestRecord2)

	n2, err = r.NextAt(off2)
	if err != nil {
		t.Fatal(err)
	}

	testRecordEquality(t, n2, TestRecord2)
}
