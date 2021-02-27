package memtable

import (
	"github.com/jo3yzhu/goveldb/internal"
	"testing"
)

func TestMemTable(t *testing.T) {
	memTable := New()
	memTable.Add(0x0000, internal.TypeValue, []byte("key"), []byte("value"))
	v, err := memTable.Get([]byte("key"));
	if err != nil {
		t.Fatal("Get error")
	}
	if string(v) != "value" {
		t.Fatal("Get error")
	}

	t.Log("memory usage:", memTable.ApproximateMemoryUsage())
}