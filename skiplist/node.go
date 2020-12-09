package skiplist

// NOTICE: each node has pointers of other nodes whose num equals level of skip list
// the 1st pointer links to next node in level 0
// the 2nd pointer links to next node in level 1
// the 3th pointer links to next node in level 2
// ...

// this is the real-world implementation of skip list based on singly link list which has no need to storage a node twice

type Node struct {
	key  interface{} // key can be any type
	next []*Node     // slice of *Node, both next and down
}

func newNode(key interface{}, height int) *Node {
	x := Node{
		key:  key,
		next: make([]*Node, height),
	}

	return &x
}

// get next node in level n
func (node *Node) getNext(level int) *Node {
	return node.next[level]
}


// set next node in level n
func (node *Node) setNext(level int, n *Node) {
	node.next[level] = n
}
