package btree

import (
	"fmt"
	"sync"
)

// BTree implements a thread-safe self-balancing search tree. It maintains sorted
// data and allows searches, sequential access, insertions, and deletions in
// logarithmic time. A BTree is specified by having a mimimum degree t, where t
// depends on disk block size or some other metric. The following properties hold
// with regard to t:
//
// - Every node except root must contain at least t-1 keys. The root may contain
// minimum 1 key.
// - All nodes (including root) may contain at most 2t – 1 keys.
// - Number of children of a node is equal to the number of keys in it plus 1.
type BTree struct {
	mu sync.RWMutex

	root      *node
	minDegree int
	size      int
	depth     int
}

// New returns a reference to a new B-Tree with a minimum degree t.
func New(t int) (*BTree, error) {
	if t < 2 {
		return nil, fmt.Errorf("minimum degree must be at least two: %d", t)
	}

	return &BTree{
		root:      newNode(),
		minDegree: t,
		depth:     1,
	}, nil
}

// Size returns the total number of nodes in the BTree.
func (bt *BTree) Size() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.size
}

// Depth returns the depth or height of the BTree.
func (bt *BTree) Depth() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.depth
}

// Search performs a lookup of the given Entry in the BTree. If the Entry exists,
// a non-nil Entry will be returned.
func (bt *BTree) Search(e Entry) Entry {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	curr := bt.root
	for curr != nil {
		found, i := curr.get(e)
		if found != nil && i >= 0 {
			return found
		}

		if curr.numChildren() == 0 {
			return nil
		}

		curr = curr.children[i]
	}

	return nil
}

// Insert inserts an Entry into the BTree. If the provided Entry is nil, then
// the method performs a no-op. If the Entry already exists, it will be replaced
// with the provided Entry. Otherwise, the new Entry will be inserted.
func (bt *BTree) Insert(e Entry) {
	if e == nil {
		return
	}

	bt.mu.Lock()
	defer bt.mu.Unlock()

	curr := bt.root

	// Traverse the tree until we've found the given entry or until we've reached
	// the leaf. When the current node is a leaf, we must have space for one extra
	// entry as we have been splitting all nodes in advance.
	for !curr.leaf() {
		found, i := curr.get(e)
		if found != nil && i >= 0 {
			// the entry already exists so we simply replace it
			curr.entries[i] = e
			return
		}

		if curr == bt.root && bt.nodeFull(curr) {
			left, right, midEntry := bt.splitRoot()

			if e.Compare(midEntry) < 0 {
				curr = left
			} else {
				curr = right
			}
		} else {
			// The entry does not exist in the current node and i denotes the child index
			// which we should search next.
			next := curr.children[i]

			if bt.nodeFull(next) {
				// Split next into left and right nodes. Change curr to point to either
				// left or right:
				//
				// If the entry is smaller than the mid entry in next, then set curr to
				// the left node. Else, set it to the right node.
				//
				// Finally, when we split next, we move the mid entry from next to its
				// parent curr.
				left, right, midEntry := next.split()

				curr.insert(midEntry)
				curr.replaceChildAt(i, left)
				curr.insertChildAt(i+1, right)
				next.clear()

				if e.Compare(midEntry) < 0 {
					curr = left
				} else {
					curr = right
				}
			} else {
				curr = next
			}
		}
	}

	curr.insert(e)
	bt.size++

	if curr == bt.root && bt.nodeFull(curr) {
		_, _, _ = bt.splitRoot()
	}
}

func (bt *BTree) splitRoot() (*node, *node, Entry) {
	left, right, midEntry := bt.root.split()
	newRoot := newNode()

	newRoot.insert(midEntry)
	newRoot.insertChildAt(0, left)
	newRoot.insertChildAt(1, right)
	bt.root.clear()

	bt.root = newRoot
	bt.depth++

	return left, right, midEntry
}

func (bt *BTree) nodeFull(n *node) bool {
	return (2*bt.minDegree)-1 == n.numEntries()
}
