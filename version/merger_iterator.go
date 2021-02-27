// Merge sort among inputs in Compaction instance is implemented by MergingIterator
// Internal keys in different sstable can be iterated in order with mergingIterator

package version

import (
	"github.com/jo3yzhu/goveldb/internal"
	"github.com/jo3yzhu/goveldb/sstable"
)

type MergingIterator struct {
	list    []*sstable.Iterator
	current *sstable.Iterator
}

func NewMergingIterator(list []*sstable.Iterator) *MergingIterator {
	return &MergingIterator{
		list: list,
	}
}

// @description: detect the iterator with smallest internal key among iterator list

func (iter *MergingIterator) findSmallest() {
	var smallest *sstable.Iterator = nil
	for i := 0; i < len(iter.list); i++ {
		if iter.list[i].Valid() {
			if smallest == nil {
				smallest = iter.list[i]
			} else {
				if internal.InternalKeyComparator(iter.list[i].InternalKey(), smallest.InternalKey()) < 0 {
					smallest = iter.list[i]
				}
			}
		}
	}

	iter.current = smallest
}

func (iter *MergingIterator) Valid() bool {
	return iter.current != nil && iter.current.Valid()
}

func (iter *MergingIterator) InternalKey() *internal.InternalKey {
	return iter.current.InternalKey()
}

func (iter *MergingIterator) SeekToFirst() {
	for i := 0; i < len(iter.list); i++ {
		iter.list[i].SeekToFirst()
	}
	iter.findSmallest()
}

func (iter *MergingIterator) Next() {
	if iter.current != nil {
		iter.current.Next() // advance the smallest iterator
	}
	iter.findSmallest() // then find the least large iterator
}
