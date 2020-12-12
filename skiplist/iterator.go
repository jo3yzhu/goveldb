package skiplist

type Iterator struct {
	list *SkipList
	node *Node
}

func (iter *Iterator) Valid() bool {
	return iter.node != nil
}

func (iter *Iterator) Key() interface{} {
	return iter.node.key
}


// @description: make the iterator step to next in level 0

func (iter *Iterator) Next() {
	iter.list.mu.RLock()
	defer iter.list.mu.RUnlock()

	iter.node = iter.node.getNext(0);
}

// @description: make the iterator step to previous in level 0

func (iter *Iterator) Prev() {
	iter.list.mu.RLock()
	defer iter.list.mu.RUnlock()

	iter.node = iter.list.findLessThan(iter.node.key)
	if iter.node == iter.list.head {
		iter.node = nil
	}
}

// @description: make the iterator step to the lower bound in level 0
// @notice: caller should ensure that if find the corresponding node

func (iter *Iterator) Seek(key interface{}) {
	iter.list.mu.RLock()
	defer iter.list.mu.RUnlock()

	iter.node, _ = iter.list.findGreaterOrEqual(key)
}

// @description: make the iterator seek to first in level 0

func (iter *Iterator) SeekToFirst() {
	iter.list.mu.RLock()
	defer iter.list.mu.RUnlock()

	iter.node = iter.list.head.getNext(0)
}

// @description: make the iterator seek to last in level 0

func (iter *Iterator) SeekToLast() {
	iter.list.mu.RLock()
	defer iter.list.mu.RUnlock()

	iter.node = iter.list.findLast()
	if iter.node == iter.list.head {
		iter.node = nil
	}
}