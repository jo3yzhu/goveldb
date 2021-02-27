package skiplist

import (
	"github.com/jo3yzhu/goveldb/utils"
	"testing"
	"time"
)

func TestSkipList_Contains(t *testing.T) {
	list := New(utils.IntComparator)
	for i := 0; i < 100; i++ {
		list.Insert(i)
	}

	for i := 0; i < 100; i++ {
		result := list.Contains(i)
		if !result {
			t.Fatal("contains test fail")
			return
		}
	}
}

func TestIterator_SeekToFirst(t *testing.T) {
	list := New(utils.IntComparator)
	for i := 0; i < 100; i++ {
		list.Insert(i)
	}
	iter := list.NewIterator()

	if iter.Valid() {
		t.Fatal("new iterator cannot be valid")
	}

	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if iter.node.key != i {
			t.Fatal("traverse error")
		}
		i++
	}
}

func TestIterator_SeekToLast(t *testing.T) {
	list := New(utils.IntComparator)
	for i := 0; i < 100; i++ {
		list.Insert(i)
	}
	iter := list.NewIterator()

	if iter.Valid() {
		t.Fatal("new iterator cannot be valid")
	}

	i := 99
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		if iter.node.key != i {
			t.Fatal("traverse error")
		}
		i--
	}
}

func TestIterator_Seek(t *testing.T) {
	length := 1000000
	slice := make([]int, 0)
	for i := 0; i < length; i++ {
		slice = append(slice, i)
	}

	sliceBefore := time.Now().Nanosecond()
	for i := 0; i < length; i++ {
		if slice[i] == length-1 {
			break;
		}
	}
	sliceAfter := time.Now().Nanosecond()
	t.Log("slice cost: ", sliceAfter-sliceBefore)

	list := New(utils.IntComparator)
	for i := 0; i < length; i++ {
		list.Insert(i)
	}

	iter := list.NewIterator()

	skipListBefore := time.Now().Nanosecond();
	iter.Seek(length - 1)

	skipListAfter := time.Now().Nanosecond();
	t.Log("skip list cost: ", skipListAfter-skipListBefore)
}
