package skiplist

import (
	"goveldb/utils"
	"math/rand"
	"sync"
)

const (
	kMaxHeight = 12
	kBranching = 4
)

type SkipList struct {
	height     int
	head       *Node
	comparator utils.Comparator
	mu         sync.RWMutex
}

func New(comp utils.Comparator) *SkipList {
	skipList := SkipList{
		height:     1,
		head:       newNode(0, kMaxHeight),
		comparator: comp,
	}
	return &skipList
}

// @description: insert a new key to skip list
// @param: the key to be inserted in skip list

func (list *SkipList) Insert(key interface{}) {
	list.mu.Lock(); // write lock
	defer list.mu.Unlock()

	_, prev := list.findGreaterOrEqual(key)
	height := list.randomHeight()

	// new node height is greater than list height, then link the head and new node in exceed level
	if height > list.height {
		for i := list.height; i < height; i++ {
			prev[i] = list.head // prev[i] == nil
		}
		list.height = height // update new height
	}

	// link new node in each level
	x := newNode(key, height)
	for i := 0; i < height; i++ {
		x.setNext(i, prev[i].getNext(i))
		prev[i].setNext(i, x)
	}

}

// @description: find out if a key exists in skip list
// @param: the key
// @return: the result

func (list *SkipList) Contains(key interface{}) bool {
	list.mu.RLock()
	defer list.mu.RUnlock()
	n, _ := list.findGreaterOrEqual(key)
	return n != nil && list.comparator(n.key, key) == 0
}

// @description: generate random height for node insertion
// @return: the random height
// @TODO: why does it look like this?

func (list *SkipList) randomHeight() int {
	height := 1
	for height < kMaxHeight && (rand.Intn(kBranching) == 0) {
		height++
	}

	return height
}

// @description: find nodes whose key is greater than param key in each level and inserting position for key
// @param: a key need to be compared while traversing list
// @return1: the first node whose key greater or equal than param key in level 0
// @return2: inserting position in each level if a node with param key need to be inserted

func (list *SkipList) findGreaterOrEqual(key interface{}) (*Node, [kMaxHeight]*Node) {
	var prev [kMaxHeight]*Node
	x := list.head
	level := list.height - 1

	for true {
		next := x.getNext(level)
		if list.keyIsAfterNode(key, next) {
			x = next
		} else {
			prev[level] = x
			if level == 0 {
				return next, prev
			} else {
				level--
			}
		}
	}

	return nil, prev
}

// @description: find out if the key less than the key of node n
// @return: if less, return true, if equal or greater, true false

func (list *SkipList) keyIsAfterNode(key interface{}, n *Node) bool {
	return (n != nil) && (list.comparator(n.key, key) < 0)
}

// @description: find last node in level 0 in O(logN) instead of O(N)
// @param: the key to be compared
// @return: the last node whose key less than param key, if key is the smallest in skip list, return head

func (list *SkipList) findLessThan(key interface{}) *Node {
	x := list.head
	level := list.height - 1

	for true {
		// find first greater than key in top level, and then sink down util level0
		next := x.getNext(level)
		if next == nil || list.comparator(next.key, key) >= 0 {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}

	return nil
}

// @description: find last node in level 0 in O(logN) instead of O(N)
// @return: the last node, if skip list is empty, return head

func (list *SkipList) findLast() *Node {
	x := list.head
	level := list.height - 1

	for true {
		next := x.getNext(level)
		if next == nil {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}

	return nil
}
