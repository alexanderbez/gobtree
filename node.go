package btree

import (
	"sort"
)

type (
	// Entries defines an alias for a slice of Entry objects.
	Entries []Entry

	// Entry defines the interface contract any element inserted into a BTree must
	// define. It is expected all concrete elements are of the same type.
	Entry interface {
		// Compare compares a receiver Entry with an Entry argument, such that 0 is
		// returned if they're equal, -1 if the receiver Entry is less than the
		// Entry argument and 1 otherwise.
		Compare(Entry) int
	}

	nodes []*node

	node struct {
		entries  Entries
		children nodes
	}
)

func newNode() *node {
	return &node{
		entries:  make(Entries, 0),
		children: make(nodes, 0),
	}
}

func (n *node) clear() {
	n.entries = nil
	n.children = nil
}

func (n *node) leaf() bool {
	return n.numChildren() == 0
}

func (n *node) numEntries() int {
	return len(n.entries)
}

func (n *node) numChildren() int {
	return len(n.children)
}

func (n *node) get(e Entry) (Entry, int) {
	// binary search for the smallest index i, s.t. n.entries[i] >= e
	i := sort.Search(n.numEntries(), func(i int) bool {
		return n.entries[i].Compare(e) >= 0 // n.entries[i] >= e
	})

	// if the index i is in bounds and equals the provided entry, return that entry
	if i < n.numEntries() && n.entries[i].Compare(e) == 0 {
		return n.entries[i], i
	}

	// the entry does not exist
	return nil, i
}

func (n *node) insert(e Entry) {
	found, i := n.get(e)
	if found != nil && i >= 0 {
		// The entry already exists in the node, so we simply overwrite it.
		n.entries[i] = e
		return
	}

	n.entries = append(n.entries, nil)
	copy(n.entries[i+1:], n.entries[i:])
	n.entries[i] = e
}

func (n *node) replaceChildAt(i int, child *node) {
	n.children[i] = child
}

func (n *node) insertChildAt(i int, child *node) {
	n.children = append(n.children, nil)
	copy(n.children[i+1:], n.children[i:])
	n.children[i] = child
}

func (n *node) split() (left *node, right *node, mid Entry) {
	midEntryIdx := n.numEntries() / 2

	leftEntries := make(Entries, len(n.entries[:midEntryIdx]))
	copy(leftEntries[:], n.entries[:midEntryIdx])

	rightEntries := make(Entries, len(n.entries[midEntryIdx+1:]))
	copy(rightEntries[:], n.entries[midEntryIdx+1:])

	leftNode := newNode()
	leftNode.entries = leftEntries

	rightNode := newNode()
	rightNode.entries = rightEntries

	if n.numChildren() > 0 {
		leftChildren := make(nodes, len(n.children[:midEntryIdx+1]))
		copy(leftChildren[:], n.children[:midEntryIdx+1])

		rightChildren := make(nodes, len(n.children[midEntryIdx+1:]))
		copy(rightChildren[:], n.children[midEntryIdx+1:])

		leftNode.children = leftChildren
		rightNode.children = rightChildren
	}

	return leftNode, rightNode, n.entries[midEntryIdx]
}
