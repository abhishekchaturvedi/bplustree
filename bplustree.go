package bplustree

import (
	"errors"
	"sync"
	"fmt"
	// "reflect"
)

// Tentative errors. Will add more.
var (
	ERR_NOT_FOUND = errors.New("key not found")
	ERR_NOT_INITIALIZED = errors.New("Tree is not initialized")
	ERR_INVALID_PARAM = errors.New("Invalid configuration parameter")
)

// This is the locker interface. The clients of the library should fill in the
// implementation for the library to use. The context to the lock/unlock calls
// should be the set of key we are operating on. The set of keys are separated
// in keys for which read lock is required vs those for which write lock is required.
// Each of the lock/unlock calls are going to be used for Insert/Delete/Lookup
// operations on the Tree.
type BplusTreeLocker interface {
	Lock(rkey []BplusTreeKey, wkey []BplusTreeKey)
	Unlock(rkey []BplusTreeKey, wkey []BplusTreeKey)
}

// A default locker implementation using sync.RWMutex.
type BplusTreeDefaultLock struct {
	mux sync.RWMutex
}
func (lck *BplusTreeDefaultLock) Lock (rkey []BplusTreeKey, wkey []BplusTreeKey) {
	lck.mux.Lock()
}
func (lck *BplusTreeDefaultLock) Unlock (rkey []BplusTreeKey, wkey []BplusTreeKey) {
	lck.mux.Unlock()
}


// The DB Manager interface needs to be implemented by the user if they want
// this BplusTree library to have a persistent backend to store/load keys from.
// 'Loader' is a function that should yield all key,value pairs iteratively for
// so that the BplusTree could be loaded with the key/values in the db when the
// BplusTree is instantiated.
// The typical usage of the loader will look like:
// for k, v := range dbMgr.Loader() {
//     // do something with the k,v, for example insert key to the bplustree
// }
// 'Insert' is the fucntion that will be called when a key is inserted to the tree.
// 'Delete' is the function that will be called when a key is deleted from the tree.
type BplusTreeDBMgr interface {
	Loader() (k BplusTreeKey, v BplusTreeElem)
	Insert(k []BplusTreeKey, v []BplusTreeElem) error
	Delete(k []BplusTreeKey) error
}

// The Memory Manager interface needs to be implemented by the user if they want
// this BplusTree library to use user provided interface for allocation. The memory
// manager could have it's own internal policy on how much to cache etc.
// 'Insert' is the fucntion that will be called when a key is inserted to the tree.
// 'Delete' is the function that will be called when a key is deleted from the tree.
type BplusTreeMemMgr interface {
	Insert(k []BplusTreeKey, v []BplusTreeElem) error
	Delete(k []BplusTreeKey) error
}

// The BplusTreeCtx struct defines the optional and necessary user-defined functions
// which will be used for certain operations.
// 'lockMgr' is the optional interface for user-defined lock/unlock fuctions.
// 'dbMgr' is the optional interface to define interaction with the persistence layer.
//         The details of the interface are defined in BplusTreeDBMgr interface.
// 'memMgr' is the interface to define the memory manager. Whenever a key/value is
//         added or removed from the BplusTree the memory needed is requested from
//         the memory manager. Memory manager can implement any policy that it may
//         need to keep the caching at the levels needed.
// 'maxDegree' is the maximum degree of the tree.
type BplusTreeCtx struct {
	lockMgr          BplusTreeLocker
	dbMgr            BplusTreeDBMgr
	memMgr           BplusTreeMemMgr
	maxDegree        int
}

// The tree itself. Contains the root and some context information.
// root of the tree. Is a node itself.
// 'context' is the context which defines interfaces used for various aspects as defined by
// BplusTreeCtx interface.
type BplusTree struct {
	root           *BplusTreeNode
	context         BplusTreeCtx
	initialized     bool
}

// user-defined key to identify the node.
// 'key' could be an arbitrary type.
// Compare function compares the given instance with the specified parameter.
// Returns -1, 0, 1 for less than, equal to and greater than respectively.
type BplusTreeKey interface {
	Compare(key BplusTreeKey) int
}


// user-defined data/content of the node. Contains the key
// at the beginning and data follows
// 'defines' the BplusTreeElemInterface which needs to define a function to get
// to the key
// 'value' is the value corresponding to this element.
type BplusTreeElem interface {
	GetKey() BplusTreeKey
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
// will search for 'foo' and return maximum of 10 keys total, 5 to the Left
// of 'foo' and 5 to the right of 'foo' as long as each of those keys when
// evaluated using the 'evaluator' returns 'true'.
type BplusTreeSearchSpecifier struct {
	searchKey       *BplusTreeKey
	direction       BplusTreeSearchDirection
	maxElems        int
	evaluator       BplusTreeKeyEvaluator
}


func BplusTreeIsEmptyInterface(x interface{}) bool {
	return x == nil
}

// Create an instance of new tree. order and ctx specify the required parameters.
// Returns pointer to the tree if successful, appropriate error otherwise.
func NewBplusTree(ctx BplusTreeCtx) (*BplusTree, error) {
	fmt.Printf("Initializing new BplusTree...\n")

	if ctx.maxDegree < 0 {
		fmt.Printf("Invalid value for degree in the context.")
		return nil, ERR_INVALID_PARAM
	}

	bplustree := &BplusTree {root: nil, initialized: true, context: ctx}

	if BplusTreeIsEmptyInterface(ctx.lockMgr) {
		fmt.Printf("No locker specified. Using default locker using mutex\n")
		bplustree.context.lockMgr = new(BplusTreeDefaultLock)
	}

	return bplustree, nil
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
