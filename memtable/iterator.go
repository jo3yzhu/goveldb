package memtable

import (
	"goveldb/internal"
	"goveldb/skiplist"
)

type Iterator struct  {
	listIterator *skiplist.Iterator
}

func (iter *Iterator) Valid() bool {
	return iter.listIterator.Valid();
}

func (iter *Iterator) InternalKey() *internal.InternalKey {
	return iter.listIterator.Key().(*internal.InternalKey)
}

func (iter *Iterator) Next() {
	iter.listIterator.Next()
}

func (iter *Iterator) Prev() {
	iter.listIterator.Prev()
}

func (iter *Iterator) Seek(target interface{}) {
	iter.listIterator.Seek(target)
}

func (iter *Iterator) SeekToFirst() {
	iter.listIterator.SeekToFirst()
}

func (iter *Iterator) SeekToLast() {
	iter.listIterator.SeekToLast()
}