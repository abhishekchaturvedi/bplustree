package bplustree

import (
	"errors"
)

// Tentative errors. Will add more.
var (
	ERR_NOT_FOUND = errors.New("key not found")
	ERR_NOT_DEFINED = errors.New("Interface is not defined")
)

// // This interface should allow us to:
// // read/write updates to tree to some backend (file, or KV store)
// // load the tree from the backend. More details later as things become more clearer.
// // To begin with it’d just be an in memory implementation because keys are actually
// // stored in the db itself. Which would mean that on startup, the tree has to be populated.
// type BplusTreeSerializer interface {
// }

// This is the locker interface. The clients of the library should fill in the implementation for the
// library to use. The context to the lock/unlock calls should be the key we are operating on.
// Each of the lock/unlock calls are going to be used for Insert/Delete/Lookup operations on the
// Tree.
type BplusTreeLocker interface {
	Lock(key* BplusTreeKey)
	Unlock(key *BplusTreeKey)
}

// Comparator for the keys. Returns -1, 0, 1 for less than, equal to and greater
// than respectively.
type BplusTreeKeyComparer interface {
	Comparator(k1 *BplusTreeKey, k2 *BplusTreeKey) int
}

// The BplusTreeCtx struct defines the optional and necessary user-defined functions
// which will be used for certain operations.
// 'locker' is the optional interface for user-defined lock/unlock fuctions.
// 'serializer' is the optional interface to define how to serialize/deserialize the blustree.
// 'comparer' is the interface to define the key comparator. This interface is required.
// 'maxDegree' is the maximum degree of the tree.
type BplusTreeCtx struct {
	locker          BplusTreeLocker
	//serializer      BplusTreeSerializer
	comparer        BplusTreeKeyComparer
	maxDegree       int
}

// The tree itself. Contains the root and some context information.
// root of the tree. Is a node itself.
// 'context' is the context which defines interfaces used for various aspects as defined by
// BplusTreeCtx interface.
type BplusTree struct {
	root           *BplusTreeNode
	context         BplusTreeCtx
}

// user-defined key to identify the node.
// 'key' could be an arbitrary type.
type BplusTreeKey struct {
	key interface{}
}

// user-defined data/content of the node. Contains the key
// at the beginning and data follows
// 'key' is the key corresponding to this element.
// 'value' is the value corresponding to this element.
type BplusTreeElem struct {
	key BplusTreeKey
	value interface{}
}

// The 'node' of the bplus tree. Each node is represented by the following data.'
// 'children' is the list of children that this node has.
// 'parent' is the pointer to the parent node.
// 'next' is the pointer to the next node (sibling to the right)
// 'prev' is the poitner to the prev node (sibling to the lef).
// 'isLeaf' whether this node is a leaf or not.
// 'content' is the 'value' part of this node which is valid only if this node is a leaf
type BplusTreeNode struct {
	children	[]*BplusTreeNode
	parent		*BplusTreeNode
	next		*BplusTreeNode
	prev		*BplusTreeNode
	isLeaf           bool
	content	        *BplusTreeElem
}

type BplusTreeSearchDirection int

const (
	Exact BplusTreeSearchDirection = 0
	Left = 1
	Right = 2
	Both = 3
)

// An interface which defines a 'evaluator' function to be used for key evaluation
// for BplusTree search logic. See more in BplusTreeSearchSpecifier
type BplusTreeKeyEvaluator interface {
	evaluator(key *BplusTreeKey) bool
}

// The BplusTreeSearchSpecifier contains user defined policy for how the
// search of a given key is to be conducted.
// searchKey specifies the exact key for which the search needs to be done.
// direction specifies the direction of the search. It could be exact, left, right or both.
// maxElems specifies the maximum number of elements that will be returned by the search
// matching the search criteria. This argument is ignored when using 'Exact' direction.
// comparer defines the comparison function to be used to while traversing keys to the left
// or right of the searchKey. This is arugment is ignored when using 'Exact' direction.
// For example:
// ss BplusTreeSearchSpecifier := {'foo', Exact, 10, MyEvalFn}
// will search for 'foo' key with exact match.
// ss BplusTreeSearchSpecifier := {'foo', Left, 10, nil}
// will search for 'foo' and return 10 keys to the left of 'foo'.
// ss BplusTreeSearchSpecifier := {'foo', Both, 10, nil}
// will search for 'foo' and return 10 keys total, 5 to the Left of 'foo' and 5 to the right of 'foo'.
// ss BplusTreeSearchSpecifier := {'foo', Both, 10, MyEvalFunc}
// will search for 'foo' and return maximum of 10 keys total, 5 to the Left of 'foo' and 5 to the right of 'foo' as long as each of those keys when evaluated using the 'evaluator' returns 'true'.

type BplusTreeSearchSpecifier struct {
	searchKey       *BplusTreeKey
	direction       BplusTreeSearchDirection
	maxElems        int
	evaluator       BplusTreeKeyEvaluator
}

// Create an instance of new tree. order and ctx specify the required parameters.
// Returns pointer to the tree if successful, appropriate error otherwise.
func NewBplusTree(order int, ctx BplusTreeCtx) (*BplusTree, error) {
	return nil, nil
}
func (bpt *BplusTree) Insert (elem BplusTreeElem) error {
	return nil
}
func (bpt *BplusTree) Remove(key BplusTreeKey) error {
	return nil
}
func (bpt *BplusTree) Search(ss BplusTreeSearchSpecifier) error {
	return nil
}
