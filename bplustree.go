package bplustree

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"
)

// Tentative errors. Will add more.
var (
	ErrNotFound       = errors.New("key not found")
	ErrNotInitialized = errors.New("Tree is not initialized")
	ErrInvalidParam   = errors.New("Invalid configuration parameter")
	ErrExists         = errors.New("Already exists")
	ErrTooLarge       = errors.New("Too many values")
)

// Locker - This is the locker interface. The clients of the library
// should fill in theimplementation for the library to use. The context to the
// lock/unlock calls should be the set of key we are operating on. Lock and Unlock
// takes the set of keys for which lock needs to be acquired as well as whether
// it is a readonly lock vs. a read-write lock.
// Each of the lock/unlock calls are going to be used for Insert/Delete/Lookup
// operations on the Tree.
type Locker interface {
	Init()
	Lock(keys []Key, readonly bool)
	Unlock(keys []Key, readonly bool)
}

// defaultLock - A default locker implementation using sync.RWMutex.
type defaultLock struct {
	mux *sync.RWMutex
}

func (lck *defaultLock) Init() {
	lck.mux = &sync.RWMutex{}
}
func (lck *defaultLock) Lock(keys []Key, readonly bool) {
	if readonly {
		lck.mux.RLock()
	} else {
		lck.mux.Lock()
	}
}
func (lck *defaultLock) Unlock(keys []Key, readonly bool) {
	if readonly {
		lck.mux.RUnlock()
	} else {
		lck.mux.Unlock()
	}
}

// DBMgr - The DB Manager interface needs to be implemented by the user if they want
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
type DBMgr interface {
	Loader() (k Key, v Element)
	Insert(k []Key, v []Element) error
	Delete(k []Key) error
}

// MemMgr - The Memory Manager interface needs to be implemented by the user if they want
// this BplusTree library to use user provided interface for allocation. The memory
// manager could have it's own internal policy on how much to cache etc.
// 'Insert' is the fucntion that will be called when a key is inserted to the tree.
type MemMgr interface {
	Alloc() error
}

// Context - This struct defines the optional and necessary user-defined functions
// which will be used for certain operations.
// 'lockMgr' is the optional interface for user-defined lock/unlock fuctions.
// 'dbMgr' is the optional interface to define interaction with the persistence layer.
//         The details of the interface are defined in BplusTreeDBMgr interface.
// 'memMgr' is the interface to define the memory manager. Whenever a key/value is
//         added or removed from the BplusTree the memory needed is requested from
//         the memory manager. Memory manager can implement any policy that it may
//         need to keep the caching at the levels needed.
// 'maxDegree' is the maximum degree of the tree.
type Context struct {
	lockMgr   Locker
	dbMgr     DBMgr
	memMgr    MemMgr
	maxDegree int
}

// BplusTree - The tree itself. Contains the root and some context information.
// root of the tree. Is a node itself.
// 'context' is the context which defines interfaces used for various aspects as defined by
// BplusTreeCtx interface.
type BplusTree struct {
	root        *treeNode
	context     Context
	initialized bool
}

// Key - user-defined key to identify the node.
// 'key' could be an arbitrary type.
// Compare function compares the given instance with the specified parameter.
// Returns -1, 0, 1 for less than, equal to and greater than respectively.
type Key interface {
	Compare(key Key) int
}

// Element - user-defined data/content of the node. Contains the key
// at the beginning and data follows
// 'defines' the BplusTreeElemInterface which needs to define a function to get
// to the key
// 'value' is the value corresponding to this element.
type Element interface {
	GetKey() Key
}

// Elements - An array of Element
type Elements []Element

// treeNode - The 'node' of the bplus tree. Each node is represented by the
// following data:
// 'children' is the list of children that this node has if not leaf node, otherwise
//            it contains the content/elements themselves.
// 'next' is the pointer to the next node (sibling to the right)
// 'prev' is the poitner to the prev node (sibling to the lef).
// 'isLeaf' whether this node is a leaf or not.
type treeNode struct {
	children Elements
	next     *treeNode
	prev     *treeNode
	isLeaf   bool
}

// treeOpType - tree operation type. Used internally.
type treeOpType int

const (
	treeOpInsert treeOpType = 0
	treeOpSearch            = 1
	treeOpRemove            = 2
)

// SearchDirection - This type defines the direction of the search. Please see
// SearchSpecifier for more details
type SearchDirection int

// SearchDirection values to be used in the SearchSpecifier when using 'Search'
// API
const (
	Exact SearchDirection = 0
	Left                  = 1
	Right                 = 2
	Both                  = 3
)

// KeyEvaluator - It is an interface which defines a 'evaluator' function to
// be used for key evaluation for BplusTree search logic. See more in
// SearchSpecifier
type KeyEvaluator interface {
	evaluator(key Key) bool
}

// SearchSpecifier - Contains user defined policy for how the
// search of a given key is to be conducted.
// searchKey specifies the exact key for which the search needs to be done.
// direction specifies the direction of the search. It could be exact, left, right or both.
// maxElems specifies the maximum number of elements that will be returned by the search
// matching the search criteria. This argument is ignored when using 'Exact' direction.
// evaluator defines the comparison function to be used to while traversing keys to the left
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
// evaluated using the 'evaluator' returns 'true'. First argument of the 'evaluator'
// function will always be the input key itself and the second argument will be the
// key with which it is being compared.
type SearchSpecifier struct {
	searchKey Key
	direction SearchDirection
	maxElems  int
	evaluator func(Key, Key) bool
}

//
// Section - Non instance internal functions.
//

// defaultAlloc - This is the default allocator. It uses the go native allocator.
// returns the allocated treeNode
func defaultAlloc() *treeNode {
	return &treeNode{}
}

// isEmptyInterface - A function to check whether the interface is empty of not.
// returns true if the interface is non-empty.
// XXX - this is probably not the accurate way of doing this?
func isEmptyInterface(x interface{}) bool {
	return x == nil
}

// getKeysToLock - accumualtes the keys for which lock may be acquired for a
// given tree operation
func getKeysToLock(key Key) []Key {
	keys := make([]Key, 1)
	keys[0] = key
	return keys
}

//
// Section - Elements instance functions.
//

// find - Find the specified key in the elements slice.
// Returns the index of element with either the matching key or the key which
// is before the key (in sort oder) if the matching key isn't found.
func (elems Elements) find(key Key) (index int, exactMatch bool) {
	index = sort.Search(len(elems), func(i int) bool {
		ret := elems[i].GetKey().Compare(key)
		if ret == 0 {
			exactMatch = true
		}
		return ret >= 0
	})
	return
}

// insert - Inserts an element to the Elements array in order.
// Returns the updated Elements slice.
func (elems Elements) insert(elem Element, maxDegree int) (Elements, error) {
	index, _ := elems.find(elem.GetKey())
	log.Printf("found element's insert position at %d, (len: %d)\n",
		index, len(elems))
	// Insert at the end case.
	if index >= len(elems) {
		log.Printf("Appending %v at %d\n", elem, index)
		elems = append(elems, elem)
		return elems, nil
	}

	// If inserting in the middle.
	// XXX: Will need to use memMgr here.
	log.Printf("Inserting %v at %d, increasing elems to (size: %d, cap: %d)\n",
		elem, index, len(elems)+1, maxDegree+1)
	newElems := make(Elements, len(elems)+1, maxDegree+1)
	copy(newElems, elems[:index])
	newElems[index] = elem
	copy(newElems[index+1:], elems[index:])
	return newElems, nil
}

// remove - remove an element from the Elements array
// Returns the updated elements slice or error if any
func (elems Elements) remove(key Key, maxDegree int) (Elements, error) {
	index, exactMatch := elems.find(key)
	if !exactMatch {
		return elems, ErrNotFound
	}

	if index >= len(elems) {
		fmt.Printf("Found element at index: %d in array of len: %d", index, len(elems))
		panic("can't have index more than number of elements")
	}

	elems = append(elems[:index], elems[index+1:]...)
	return elems, nil
}

// String - string representation of the 'Elements'
// returns the string representation of the 'Elements'
func (elems Elements) String() string {
	var elemStr []string
	for _, elem := range elems {
		switch elemType := elem.(type) {
		case *treeNode:
			elemStr = append(elemStr, fmt.Sprintf(" N <%v> ", elemType.GetKey()))
		default:
			elemStr = append(elemStr, fmt.Sprintf(" L <%v> ", elemType.GetKey()))
		}
	}
	return fmt.Sprintf("%v", elemStr)
}

//
// Section - BplusTreeNode instance functions.
//

// insertElement - insert an element into the Elements of a given treeNode.
// returns appropriate error if insert fails.
func (node *treeNode) insertElement(elem Element, maxDegree int) error {
	children, err := node.children.insert(elem, maxDegree)
	if err != nil {
		return err
	}

	node.children = children
	return nil
}

// removeElement - remove an element from the Elements of given treeNode
// Returns appropriate error if remove fails.
func (node *treeNode) removeElement(key Key, maxDegree int) error {
	children, err := node.children.remove(key, maxDegree)
	if err != nil {
		return err
	}
	node.children = children
	return nil
}

// GetKey - Each treeNode has to implement the 'Element' interface as well because
// at the leaf level, a treeNode is an element. However, when used, a treeNode may
// or may not be a leaf node. Thus the GetKey implementation gives the lowest key
// in the subtree rooted at treeNode.
// Retuns the lowest key from the subtree roooted at 'treeNode'
func (node *treeNode) GetKey() Key {
	var tmpNode *treeNode = node
	for !tmpNode.isLeaf {
		tmpNode = tmpNode.children[0].(*treeNode)
	}
	return tmpNode.children[0].GetKey()
}

// String - stringigy treeNode
// Returns the string representation of the treeNode.
func (node *treeNode) String() string {
	var prevKey, nextKey string
	if node.prev != nil {
		prevKey = fmt.Sprintf("%v", node.prev.GetKey())
	} else {
		prevKey = "nil"
	}

	if node.next != nil {
		nextKey = fmt.Sprintf("%v", node.next.GetKey())
	} else {
		nextKey = "nil"
	}

	return fmt.Sprintf("{%p [children: %v, prev: %v, next: %v, leaf: %v]}",
		node, node.children, prevKey, nextKey, node.isLeaf)
}

//
// Section - BplusTree instance functions.
//

// treeNodeInit - initializer for a given treeNode
func (bpt *BplusTree) treeNodeInit(
	node *treeNode,
	isLeaf bool,
	next *treeNode,
	prev *treeNode,
	initLen int) {

	node.children = make([]Element, initLen, bpt.context.maxDegree)
	node.isLeaf = isLeaf
	node.next = next
	node.prev = prev
}

// indexResetter - decrements the index and resets if needed. Note that
// when we are looking for a key to 'insert' another key, we need to
// always return the previous key. However, when we are looking for
//'search' operation, we are interested in the exact key and hence we
// don't need to decrement the index in that case.
// Returns the new index
func indexResetter(index int, op treeOpType, exactMatch bool) int {
	switch op {
	case treeOpInsert:
		index--
	case treeOpSearch, treeOpRemove:
		if !exactMatch {
			index--
		}
	}
	if index <= 0 {
		index = 0
	}
	return index
}

// find - find a given key in the BplusTree. The function gathers the list of treeNodes
// found in the path to the leaf node which is equal or lesser than the provided key.
// Returns the list of nodes gathered on the path, and error if any.
func (bpt *BplusTree) find(key Key,
	op treeOpType,
	resetter func(int, treeOpType, bool) int) (nodes []*treeNode, indexes []int, err error) {
	nodes = make([]*treeNode, 0) // We don't know the capacity.
	if op == treeOpRemove {
		indexes = make([]int, 0)
	}
	node := bpt.root
	err = nil
	if node == nil {
		return nil, nil, ErrNotFound
	}

	for node != nil {
		if node.isLeaf {
			nodes = append(nodes, node)
			break
		}
		// Only accumulate nodes if this is find for 'insert' or 'remove'.
		// For search, we are only interested in the leaf node.
		if op == treeOpInsert || op == treeOpRemove {
			nodes = append(nodes, node)
		}

		cnodes := node.children
		exactMatch := false
		index := sort.Search(len(cnodes), func(i int) bool {
			ret := cnodes[i].GetKey().Compare(key)
			if ret == 0 {
				exactMatch = true
			}
			return ret >= 0 // Every key past this point is >=0
		})

		index = resetter(index, op, exactMatch)
		if op == treeOpRemove {
			indexes = append(indexes, index)
		}
		node = cnodes[index].(*treeNode)
	}
	return
}

// insertFinder - Function to find the path of nodes in the BplusTree where the given
// key needs to be inserted.
// Retuns the list of nodes which are encoutered on the path to the leaf node which is
// smaller in order to the specified key.
func (bpt *BplusTree) insertFinder(key Key) (nodes []*treeNode, indexes []int, err error) {
	return bpt.find(key, treeOpInsert, indexResetter)
}

// searchFinder - This function looks for the specified key and returns the leaf node
// which contains the key
func (bpt *BplusTree) searchFinder(key Key) (nodes []*treeNode, indexes []int, err error) {
	return bpt.find(key, treeOpSearch, indexResetter)
}

// removeFinder - This function looks for the specified key and returns all nodes on the
// path to the leaf node which contains the key
func (bpt *BplusTree) removeFinder(key Key) (nodes []*treeNode, indexes []int, err error) {
	return bpt.find(key, treeOpRemove, indexResetter)
}

// rebalance - rebalances the tree once a new node is inserted.
// returns error if any
func (bpt *BplusTree) rebalance(nodes []*treeNode) (err error) {
	numNodes := len(nodes)

	var parent, curr, next *treeNode

	switch {
	case numNodes == 1:
		curr = nodes[0]
		// XXX use memMgr.
		parent = defaultAlloc()
		bpt.treeNodeInit(parent, false, nil, nil, 0)
		parent.children = append(parent.children, curr)
		// Make the new node the parent, and everything else accumulated
		// so far becomes the children.
		bpt.root = parent
	default:
		parent = nodes[numNodes-2]
		curr = nodes[numNodes-1]
	}

	currChildren := curr.children
	midp := len(currChildren) / 2

	// XXX: Use memMgr.
	next = defaultAlloc()
	bpt.treeNodeInit(next, curr.isLeaf, curr.next, curr, len(currChildren)-midp)
	curr.children = currChildren[:midp]
	copy(next.children, currChildren[midp:])
	curr.next = next

	if next.next != nil {
		next.next.prev = next
	}

	err = parent.insertElement(next, bpt.context.maxDegree)
	return
}

// checkRebalance - Check to see if any rebalance is needed on the nodes traversed
// through insertion of a new element
// Returns error if encountered.
func (bpt *BplusTree) checkRebalance(nodes []*treeNode) (err error) {
	// Traverse in reverse order to address the nodes towards leaves first.
	for i := len(nodes) - 1; i >= 0; i-- {
		node := nodes[i]
		degree := bpt.context.maxDegree

		if len(node.children) > degree {
			log.Printf("node %v (leaf: %v) has degree (%d) more than allowed, splitting\n",
				node, node.isLeaf, len(node.children))
			err = bpt.rebalance(nodes[:i+1])
		} else {
			log.Printf("node %v (leaf: %v) has degree (%d). Allowed\n",
				node, node.isLeaf, len(node.children))
		}
	}
	return
}

// merge - merge siblings as needed after a node is removed.
func (bpt *BplusTree) merge(parent *treeNode,
	siblIndex int,
	curr *treeNode,
	sibling *treeNode,
	direction SearchDirection) error {

	// If merging with right sibling then append right sibling's chilren
	// to current node, otherwise append current nodes' children to left
	// sibling.
	switch {
	case direction == Right:
		fmt.Printf("Merging %v and %v\n", curr, sibling)
		curr.children = append(curr.children, sibling.children...)
		curr.next = sibling.next
		// drop the entry of the right node from parent's children.
		parent.children = append(parent.children[:siblIndex],
			parent.children[siblIndex+1:]...)
	case direction == Left:
		fmt.Printf("Merging %v and %v\n", sibling, curr)
		sibling.children = append(sibling.children, curr.children...)
		sibling.next = curr.next
		// drop the entry of the current node from parent's children.
		if siblIndex == (len(parent.children) - 2) { // current node is the rightmost node.
			parent.children = parent.children[:siblIndex+1]
		} else {
			parent.children = append(parent.children[:siblIndex+1],
				parent.children[siblIndex+2:]...)
		}
	default: // Merging with parent.
		fmt.Printf("Merging Parent %v and %v\n", parent, curr)
		parent.children = curr.children
		parent.isLeaf = curr.isLeaf
		return nil
	}

	return nil
}

// distribute -- redistribute nodes among siblings for balancing the tree when a node
// is removed. This function will make the # of children equal (off by 1 max) in the
// current node and the sibling node.
func (bpt *BplusTree) distribute(parent *treeNode,
	siblIndex int,
	curr *treeNode,
	sibling *treeNode,
	direction SearchDirection) error {

	// Distribute the children from sibling to the current node such
	// that both of them have equal lengths.
	numChildrenToDistribute := (len(sibling.children) -
		(len(sibling.children)+len(curr.children))/2)
	switch {
	case direction == Right:
		fmt.Printf("Distributing %d elements from right sibling %v and %v\n",
			numChildrenToDistribute, sibling, curr)
		curr.children = append(curr.children, sibling.children[:numChildrenToDistribute]...)
		sibling.children = sibling.children[numChildrenToDistribute:]
	case direction == Left:
		fmt.Printf("Distributing %d elements from left sibling %v and %v\n",
			numChildrenToDistribute, sibling, curr)
		nLeft := len(sibling.children)
		tmpChildren := sibling.children[nLeft-numChildrenToDistribute:]
		sibling.children = sibling.children[:nLeft-numChildrenToDistribute]
		curr.children = append(tmpChildren, curr.children...)
	default:
		panic("unexpected condition")
	}
	return nil
}

// checkMerge - Check to see if any merging is needed on the nodes traversed
// through deletion after an element is deleted. Following are the scenarios where
// we'd need redistributing or merging. If the current node's number of children
// is less than 1/2 of allowed degree (d), and nLeft and nRight be the # of children
// that the left and right sibling nodes have, then:
// nLeft		nRight		Action
// nil			nil			Merge with parent
// nil 			<d/2		Merge with right sibling
// nil          >d/2        Distribute with right sibling
// <d/2			nil			Merge with left sibling
// <d/2			<d/2		Merge with sibling which has less children
// <d/2 		>d/2		Merge with left sibling
// >d/2 		nil			Distribute with left sibling
// >d/2			<d/2		Merge with right sibling
// >d/2			>d/2		Distribute with sibling whichever has less children
// Returns error if encountered.
func (bpt *BplusTree) checkMergeOrDistribute(nodes []*treeNode, indexes []int) (err error) {
	// Traverse in reverse order to address the nodes towards leaves first.
	// Note the iteration to 1, as nothing to be done for the root node if the degree
	// drops below d/2
	for i := len(nodes) - 1; i >= 1; i-- {
		node := nodes[i]
		minDegree := bpt.context.maxDegree / 2
		if len(node.children) >= minDegree {
			break
		}

		log.Printf("node %v (leaf: %v) has degree (%d) less than allowed. "+
			"Needs merging or distributing\n", node, node.isLeaf, len(node.children))

		parent := nodes[i-1]
		var leftSibl, rightSibl *treeNode = nil, nil
		var nLeft, nRight int = 0, 0
		var lIndex, rIndex int = 0, 0
		if indexes[i-1] != 0 {
			lIndex = indexes[i-1] - 1
			leftSibl = parent.children[lIndex].(*treeNode)
			nLeft = len(leftSibl.children)
		}
		if indexes[i-1] < (len(parent.children) - 1) {
			rIndex = indexes[i-1] + 1
			rightSibl = parent.children[rIndex].(*treeNode)
			nRight = len(rightSibl.children)
		}
		switch {
		case nLeft == 0 && nRight == 0:
			err = bpt.merge(parent, 0, node, nil, Exact)
		case nLeft == 0 && nRight <= minDegree:
			err = bpt.merge(parent, rIndex, node, rightSibl, Right)
		case nLeft == 0 && nRight > minDegree:
			err = bpt.distribute(parent, rIndex, node, rightSibl, Right)
		case nLeft <= minDegree && nRight == 0:
			err = bpt.merge(parent, lIndex, node, leftSibl, Left)
		case nLeft <= minDegree && nRight <= minDegree:
			sibl := leftSibl
			direction := SearchDirection(Left)
			ind := lIndex
			if nRight < nLeft {
				sibl = rightSibl
				direction = Right
				ind = rIndex
			}
			err = bpt.merge(parent, ind, node, sibl, direction)
		case nLeft <= minDegree && nRight > minDegree:
			err = bpt.merge(parent, lIndex, node, leftSibl, Left)
		case nLeft > minDegree && nRight == 0:
			err = bpt.distribute(parent, lIndex, node, leftSibl, Left)
		case nLeft > minDegree && nRight <= minDegree:
			err = bpt.merge(parent, rIndex, node, rightSibl, Right)
		case nLeft > minDegree && nRight > minDegree:
			sibl := leftSibl
			direction := SearchDirection(Left)
			ind := lIndex
			if nRight < nLeft {
				sibl = rightSibl
				direction = Right
				ind = rIndex
			}
			err = bpt.distribute(parent, ind, node, sibl, direction)
		}
	}
	return
}

// writeLayout - Writes the tree layout on the provided writer
func (bpt *BplusTree) writeLayout(writer io.Writer) {
	leafIdx := 0
	nodeIdx := 0
	levelIdx := 0

	if !bpt.initialized || bpt.root == nil {
		return
	}

	fmt.Fprintf(writer, "Dumping the tree layout.. numChildren: %d\n", len(bpt.root.children))
	nodeList := bpt.root.children
	nodeLensList := make([]int, 1)
	nodeLensList[0] = len(bpt.root.children)
	numElems := nodeLensList[0]
	printLevel := true
	fmt.Fprintf(writer, "LEVEL -- 0    <root: %v>\n", bpt.root)
	for i := 0; i < numElems; i++ {
		if printLevel {
			fmt.Fprintf(writer, "LEVEL -- %d    ", levelIdx+1)
			printLevel = false
		}
		node := nodeList[i]
		switch elemType := node.(type) {
		case *treeNode:
			if elemType.isLeaf {
				fmt.Fprintf(writer, "<tree-L-node :%d, node: %v> ",
					leafIdx, elemType)
				leafIdx++
			} else {
				fmt.Fprintf(writer, "<tree-I-node :%d, node: %v> ",
					nodeIdx, elemType)
				nodeList = append(nodeList, elemType.children...)
				nodeLensList = append(nodeLensList, len(elemType.children))
				numElems += len(elemType.children)
			}
		default:
			fmt.Fprintf(writer, "<elem-node :%d, node: %v> ",
				nodeIdx, elemType)
		}
		nodeIdx++
		if nodeIdx >= nodeLensList[levelIdx] {
			levelIdx++
			nodeIdx = 0
			fmt.Fprintf(writer, "\n")
			printLevel = true
		}
	}
	fmt.Fprintf(writer, "DONE.. dumping the layout\n")
	fmt.Fprintf(writer, "----------------------------\n")
}

// writeTree - writes the tree (including the layout if requested) to the provided
// writer.
func (bpt *BplusTree) writeTree(writer io.Writer, printLayout bool) error {
	node := bpt.root
	// Print tree layout.
	if printLayout == true {
		bpt.writeLayout(writer)
	}

	// Go to the left most leaf node and start printing in order.
	for node != nil {
		if node.isLeaf {
			break
		}
		node = node.children[0].(*treeNode)
	}

	if node == nil {
		fmt.Fprintln(writer, "Tree is empty")
		return nil
	}

	index := 0
	for node != nil {
		fmt.Fprintf(writer, "leaf node: %d\n", index)
		for _, child := range node.children {
			fmt.Fprintf(writer, "\t%v\n", child)
		}

		node = node.next
		index++
	}
	return nil
}

//
// External functions.
//

// NewBplusTree - Create an instance of new tree. order and ctx specify the
// required parameters.
// Returns pointer to the tree if successful, appropriate error otherwise.
func NewBplusTree(ctx Context) (*BplusTree, error) {
	log.Printf("Initializing new BplusTree...\n")

	if ctx.maxDegree < 0 {
		log.Printf("Invalid value for degree in the context.")
		return nil, ErrInvalidParam
	}

	// XXX: Use memory mgr to alloc.
	// XXX: Use dbMgr to load to initialize.
	bplustree := &BplusTree{root: nil, initialized: true, context: ctx}

	if isEmptyInterface(ctx.lockMgr) {
		log.Printf("No locker specified. Using default locker using mutex\n")
		bplustree.context.lockMgr = new(defaultLock)
		bplustree.context.lockMgr.Init()
	}

	return bplustree, nil
}

// Insert - Insert an element to the tree. The 'elem' specified needs to
// extend the 'Element' interface.
func (bpt *BplusTree) Insert(elem Element) error {
	if !bpt.initialized {
		return ErrNotInitialized
	}

	keys := getKeysToLock(elem.GetKey())
	bpt.context.lockMgr.Lock(keys, false)
	defer bpt.context.lockMgr.Unlock(keys, false)

	log.Printf("Inserting %v\n", elem)

	if bpt.root == nil {
		newnode := defaultAlloc()
		bpt.treeNodeInit(newnode, true, nil, nil, 0)
		newnode.children = append(newnode.children, elem)
		bpt.root = newnode
		log.Printf("Done..... Inserting %v\n", elem)
		return nil
	}

	nodes, _, err := bpt.insertFinder(elem.GetKey())
	if err != nil {
		log.Printf("Inserting %v encountered error: %v\n", elem, err)
		return err
	}

	nodeToInsertAt := nodes[len(nodes)-1]
	err = nodeToInsertAt.insertElement(elem, bpt.context.maxDegree)
	if err != nil {
		log.Printf("Inserting %v encountered error: %v\n", elem, err)
		return err
	}

	err = bpt.checkRebalance(nodes)
	log.Printf("Done..... Inserting %v. Printing...\n", elem)
	bpt.Print()
	log.Printf("Done..... Printing after insert of %v\n", elem)
	return err
}

// Remove - Remove an element with the given key from the tree.
// Returns error if encoutered
func (bpt *BplusTree) Remove(key Key) error {
	keys := getKeysToLock(key)
	bpt.context.lockMgr.Lock(keys, false)
	defer bpt.context.lockMgr.Unlock(keys, false)

	if !bpt.initialized {
		log.Printf("Tree not initialized")
		return ErrNotInitialized
	}

	nodes, indexes, err := bpt.removeFinder(key)
	if err != nil {
		log.Printf("failed to find key %v to remove", key)
		return err
	}

	log.Printf("Accumulated following nodes\n")
	for _, n := range nodes {
		log.Printf("<%v> ", n)
	}
	log.Printf("\n----------------------------\n")

	nodeToRemoveFrom := nodes[len(nodes)-1]

	err = nodeToRemoveFrom.removeElement(key, bpt.context.maxDegree)
	if err != nil {
		fmt.Printf("Failed to remove element from the elements list: %v", err)
		return err
	}

	err = bpt.checkMergeOrDistribute(nodes, indexes)
	log.Printf("Done..... Removing %v. Printing...\n", key)
	bpt.Print()
	log.Printf("Done..... Printing after remove of %v\n", key)
	return err
}

// Search - Search for a given key (or more) in the tree. The 'ss' (SearchSpecifier)
// argument specifies what needs to be searched. Please look at SearchSpecifier for
// more details on specifying keys to search for.
// Returns the slice of elements matchig the search criteria or error if encountered.
func (bpt *BplusTree) Search(ss SearchSpecifier) (result []Element, err error) {
	// Lets initialize the lock. We only need a read lock here.
	keys := getKeysToLock(ss.searchKey)
	bpt.context.lockMgr.Lock(keys, true)
	defer bpt.context.lockMgr.Unlock(keys, true)

	// Return error if not initialized.
	if !bpt.initialized {
		log.Printf("Tree is not initialized\n")
		return nil, ErrNotInitialized
	}

	// XXX: Do some more sanity check for the input parameters.
	// What should be the max limit of # of elements? Using 50 for now.
	// Better would be to curtail the elems to 50? For now, return error.
	if ss.maxElems > 50 {
		log.Printf("maxElems too large\n")
		return nil, ErrTooLarge
	}

	// Get the leaf node where the element should be.
	nodes, _, err := bpt.searchFinder(ss.searchKey)
	if err != nil {
		log.Printf("Failed to find key: %v\n", ss.searchKey)
		return nil, err
	}

	if len(nodes) != 1 {
		panic("unexpected length")
	}

	node := nodes[0]

	// Do binary search in the leaf node.
	index, _ := node.children.find(ss.searchKey)
	if index >= len(node.children) {
		log.Printf("Failed to find key: %v\n", ss.searchKey)
		return nil, ErrNotFound
	}

	matchingElem := node.children[index]

	if matchingElem == nil {
		log.Printf("Failed to find key: %v\n", ss.searchKey)
		return nil, ErrNotFound
	}

	// If search is exact then we already found the element. Return that.
	if ss.direction == Exact {
		result = make([]Element, 1)
		result[0] = matchingElem
		return result, nil
	}

	// Figure out how many elements to the left and/or right are to be
	// accumulated.
	numElemsLeft := 0
	numElemsRight := 0
	switch {
	case ss.direction == Right:
		numElemsRight = ss.maxElems
	case ss.direction == Left:
		numElemsLeft = ss.maxElems
	case ss.direction == Both:
		numElemsLeft = ss.maxElems / 2
		numElemsRight = ss.maxElems / 2
	}

	// Check to see if evaluator is in use.
	ignoreEvaluator := false
	if ss.evaluator == nil {
		ignoreEvaluator = true
	}
	evaluatorLeftExhausted := false
	evaluatorRightExhausted := false

	result = make([]Element, 1, ss.maxElems)
	result[0] = matchingElem

	// Setup the left index/markers to start accumulating elements.
	leftStart := 0
	leftEnd := index
	leftNode := node
	leftIndex := leftEnd - 1
	// If the exact match is at the beginning of a leaf, then the elements
	// to the left are in 'prev' leaf. Adjust for that.
	if index == 0 {
		leftNode = node.prev
		if leftNode != nil {
			leftEnd = len(leftNode.children)
			leftIndex = len(leftNode.children) - 1
		}
	}

	// Setup right index/markers.
	rightStart := index + 1
	rightNode := node
	rightIndex := index + 1
	// If the exact match is at the end of a leaf, then the elements
	// to the right are in 'next' leaf. Adjust for that.
	if index == len(node.children)-1 {
		rightNode = node.next
		if rightNode != nil {
			rightStart = 0
			rightIndex = 0
		}
	}

	// If we are not using the evaluator, we can accumulate in batches until
	// we have accumulated enough to meet the left and right width
	if ignoreEvaluator {
		// accumulating as many elements as needed to the left of exact match
		for numElemsLeft > 0 && leftNode != nil {
			if leftEnd == -1 {
				leftEnd = len(leftNode.children)
			}
			if (leftEnd - leftStart) < numElemsLeft {
				result = append(leftNode.children[leftStart:leftEnd], result...)
				numElemsLeft -= leftEnd - leftStart
				leftEnd = -1 // We need to reset it.
				leftNode = leftNode.prev
			} else {
				result = append(leftNode.children[leftEnd-numElemsLeft:leftEnd], result...)
				break
			}
		}
		// accumulating as many elements as needed to the right of exact match
		for numElemsRight > 0 && rightNode != nil {
			rightEnd := len(rightNode.children)
			if (rightEnd - rightStart) < numElemsRight {
				result = append(result, rightNode.children[rightStart:rightEnd]...)
				numElemsRight -= rightEnd - rightStart
				rightStart = 0
				rightNode = rightNode.next
			} else {
				result = append(result, rightNode.children[rightStart:rightStart+numElemsRight]...)
				break
			}
		}
	} else {
		// Else case: If the evaluator is specified however, we need to traverse linearly
		// from the exact match to either accumulate as many elements (as per the maxElems
		// from left and/or right) or stop when the evaluator stops evaluating to true even
		// if we haven't accumulated 'maxElems'.

		// Do it for the left side
		for numElemsLeft > 0 && leftNode != nil {
			elemToLeft := leftNode.children[leftIndex]
			evaluatorLeftExhausted = !ss.evaluator(ss.searchKey, elemToLeft.GetKey())
			if evaluatorLeftExhausted {
				break
			}
			result = append([]Element{elemToLeft}, result...)
			leftIndex--
			numElemsLeft--
			if leftIndex < 0 {
				leftNode = leftNode.prev
				if leftNode != nil {
					leftIndex = len(leftNode.children) - 1
				}
			}
		}

		// Do it for the right side.
		for numElemsRight > 0 && rightNode != nil {
			elemToRight := rightNode.children[rightIndex]
			evaluatorRightExhausted = !ss.evaluator(ss.searchKey, elemToRight.GetKey())
			if evaluatorRightExhausted {
				break
			}
			result = append(result, elemToRight)
			rightIndex++
			numElemsRight--
			if rightIndex >= len(rightNode.children) {
				rightNode = rightNode.next
				if rightNode != nil {
					rightIndex = 0
				}
			}
		}
	}

	return
}

// This function works as a search iterator, where no evaluator is expected in the
// search specifier. Instead, only a key is specified. Once the caller retrieves the
// 'result' (a BplusTreeSearchResult instance), they can then call, the GetNext or GetPrev
// Apis to fetch the next key themselves. The result object contains the lock which the caller
// needs to use while traversing.
// func (bpt *BplusTree) SearchIterator(ss BplusTreeSearchSpecifier) (result BplusTreeSearchResult, err error) {

// }

// Print - Prints the BplusTree on stdout.
func (bpt *BplusTree) Print() error {
	return bpt.writeTree(os.Stdout, false, true)
}
