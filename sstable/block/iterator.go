package block

import "github.com/jo3yzhu/goveldb/internal"

type Iterator struct {
	block *Block
	index int
}

func (iter *Iterator) Valid() bool {
	return iter.index >= 0 && iter.index < len(iter.block.items)
}

func (iter *Iterator) InternalKey() *internal.InternalKey {
	return &iter.block.items[iter.index]
}

func (iter *Iterator) Next() {
	iter.index++
}

func (iter *Iterator) Prev() {
	iter.index--
}

// @description: seek to first element >= target in one block using binary search by UserKey comparing
// 				 if such element doesn't exist, the iterator will be set at len(iter.block.items), which makes this iterator invalid
// @params: UserKey need to be indexed

func (iter *Iterator) Seek(target interface{}) {
	left := 0
	right := len(iter.block.items) - 1

	// unclosed section
	for left < right {
		mid := (left + right) / 2
		if internal.UserKeyComparator(iter.block.items[mid].UserKey, target) < 0 { // if num[i] < target
			left = mid + 1
		} else {
			right = mid
		}
	}

	// special case
	if left == len(iter.block.items) - 1 {
		if internal.UserKeyComparator(iter.block.items[left].UserKey, target) < 0 { // if the largest element is smaller than target, not found
			left++ // iterator is invalid now
		}
	}

	iter.index = left
}

func (iter *Iterator) SeekToFirst() {
	iter.index = 0
}

func (iter *Iterator) SeekToLast() {
	if len(iter.block.items) > 0 {
		iter.index = len(iter.block.items) - 1
	}
}