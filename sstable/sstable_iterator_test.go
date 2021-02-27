package sstable

import (
	"github.com/jo3yzhu/goveldb/internal"
	"testing"
)

func Test_SsTable_Iterator(t *testing.T) {
	builder := NewTableBuilder("000123.ldb")
	item := internal.NewInternalKey(1, internal.TypeValue, []byte("123"), []byte("1234"))
	builder.Add(item)
	item = internal.NewInternalKey(2, internal.TypeValue, []byte("124"), []byte("1245"))
	builder.Add(item)
	item = internal.NewInternalKey(3, internal.TypeValue, []byte("125"), []byte("0245"))
	builder.Add(item)
	err := builder.Finish()
	if err != nil {
		t.Fail()
		return
	}

	table, err := Open("000123.ldb")
	if err != nil {
		t.Fail()
		return
	}
	it := table.NewIterator()

	// seek lexicographically
	it.Seek([]byte("1240000"))
	if it.Valid() {
		if string(it.InternalKey().UserKey) != "125" {
			t.Fatal(it.InternalKey().UserKey)
		}
	} else {
		t.Fail()
	}
}
