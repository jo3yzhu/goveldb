package block

import (
	"github.com/jo3yzhu/goveldb/internal"
	"testing"
)

func Test_SsTable(t *testing.T) {
	var builder BlockBuilder

	item := internal.NewInternalKey(1, internal.TypeValue, []byte("123"), []byte("321"))
	builder.Add(item)
	item = internal.NewInternalKey(2, internal.TypeValue, []byte("456"), []byte("654"))
	builder.Add(item)
	item = internal.NewInternalKey(3, internal.TypeValue, []byte("789"), []byte("987"))
	builder.Add(item)

	p := builder.Finish()
	block := New(p)

	iter := block.NewIterator()

	iter.Seek([]byte("123"))
	if iter.Valid() {
		if string(iter.InternalKey().UserKey) != "123" {
			t.Fail()
		}
	} else {
		t.Fail()
	}

	iter.Seek([]byte("124"))
	if iter.Valid() {
		if string(iter.InternalKey().UserKey) != "456" {
			t.Fail()
		}
	} else {
		t.Fail()
	}

	iter.Seek([]byte("788"))
	if iter.Valid() {
		if string(iter.InternalKey().UserKey) != "789" {
			t.Fail()
		}
	} else {
		t.Fail()
	}

	iter.Seek([]byte("790"))
	if iter.Valid() {
		t.Fail()
	}
}
