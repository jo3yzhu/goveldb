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

func (iter *Iterator) Next() {
	iter.list.mu.RLock()
	defer iter.list.mu.RUnlock()

	iter.node = iter.node.getNext(0);
}

func (iter *Iterator) Prev() {
	iter.list.mu.RLock()
	defer iter.list.mu.RUnlock()

	iter.node = iter.list.findLessThan(iter.node.key)
	if iter.node == iter.list.head {
		iter.node = nil
	}
}

func (iter *Iterator) Seek(key interface{}) {
	iter.list.mu.RLock()
	defer iter.list.mu.RUnlock()

	iter.node, _ = iter.list.findGreaterOrEqual(key)
}

func (iter *Iterator) SeekToFirst() {
	iter.list.mu.RLock()
	defer iter.list.mu.RUnlock()

	iter.node = iter.list.head.getNext(0)
}

func (iter *Iterator) SeekToLast() {
	iter.list.mu.RLock()
	defer iter.list.mu.RUnlock()

	iter.node = iter.list.findLast()
	if iter.node == iter.list.head {
		iter.node = nil
	}
}