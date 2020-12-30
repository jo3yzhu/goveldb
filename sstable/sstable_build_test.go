package sstable

import (
	"goveldb/internal"
	"testing"
)

func Test_SsTable_Build(t *testing.T) {
	builder := NewTableBuilder("./builder.db")
	item := internal.NewInternalKey(1, internal.TypeValue, []byte("123"), []byte("1234"))
	builder.Add(item)
	item = internal.NewInternalKey(2, internal.TypeValue, []byte("124"), []byte("1245"))
	builder.Add(item)
	item = internal.NewInternalKey(3, internal.TypeValue, []byte("125"), []byte("0245"))
	builder.Add(item)
	err := builder.Finish()
	if err != nil {
		t.Fail()
	}
}