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
	ERR_NOT_FOUND       = errors.New("key not found")
	ERR_NOT_INITIALIZED = errors.New("Tree is not initialized")
	ERR_INVALID_PARAM   = errors.New("Invalid configuration parameter")
	ERR_EXISTS          = errors.New("Already exists")
	ERR_TOO_LARGE       = errors.New("Too many values")
)

// This is the locker interface. The clients of the library should fill in the
// implementation for the library to use. The context to the lock/unlock calls
// should be the set of key we are operating on. The set of keys are separated
// in keys for which read lock is required vs those for which write lock is required.
// Each of the lock/unlock calls are going to be used for Insert/Delete/Lookup
// operations on the Tree.
type BplusTreeLocker interface {
	Init()
	Lock(keys []BplusTreeKey, readonly bool)
	Unlock(keys []BplusTreeKey, readonly bool)
}

// A default locker implementation using sync.RWMutex.
type BplusTreeDefaultLock struct {
	mux *sync.RWMutex
}

func (lck *BplusTreeDefaultLock) Init() {
	lck.mux = &sync.RWMutex{}
}
func (lck *BplusTreeDefaultLock) Lock(keys []BplusTreeKey, readonly bool) {
	if readonly {
		lck.mux.RLock()
	} else {
		lck.mux.Lock()
	}
}
func (lck *BplusTreeDefaultLock) Unlock(keys []BplusTreeKey, readonly bool) {
	if readonly {
		lck.mux.RUnlock()
	} else {
		lck.mux.Unlock()
	}
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
type BplusTreeMemMgr interface {
	Alloc() error
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
	lockMgr   BplusTreeLocker
	dbMgr     BplusTreeDBMgr
	memMgr    BplusTreeMemMgr
	maxDegree int
}

// The tree itself. Contains the root and some context information.
// root of the tree. Is a node itself.
// 'context' is the context which defines interfaces used for various aspects as defined by
// BplusTreeCtx interface.
type BplusTree struct {
	root        *BplusTreeNode
	context     BplusTreeCtx
	initialized bool
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

type BplusTreeElems []BplusTreeElem

// The 'node' of the bplus tree. Each node is represented by the following data.'
// 'children' is the list of children that this node has if not leaf node, otherwise
//            it contains the content.
// 'next' is the pointer to the next node (sibling to the right)
// 'prev' is the poitner to the prev node (sibling to the lef).
// 'isLeaf' whether this node is a leaf or not.
type BplusTreeNode struct {
	children BplusTreeElems
	next     *BplusTreeNode
	prev     *BplusTreeNode
	isLeaf   bool
}

type BplusTreeSearchDirection int

const (
	Exact BplusTreeSearchDirection = 0
	Left                           = 1
	Right                          = 2
	Both                           = 3
)

// An interface which defines a 'evaluator' function to be used for key evaluation
// for BplusTree search logic. See more in BplusTreeSearchSpecifier
type BplusTreeKeyEvaluator interface {
	evaluator(key BplusTreeKey) bool
}

// The BplusTreeSearchSpecifier contains user defined policy for how the
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
type BplusTreeSearchSpecifier struct {
	searchKey BplusTreeKey
	direction BplusTreeSearchDirection
	maxElems  int
	evaluator func(BplusTreeKey, BplusTreeKey) bool
}

// Non instance functions.
func BplusTreeDefaultAlloc() *BplusTreeNode {
	return &BplusTreeNode{}
}
func BplusTreeIsEmptyInterface(x interface{}) bool {
	return x == nil
}

// BplusTreeElems instance functions.
func (elems BplusTreeElems) find(key BplusTreeKey) (index int) {
	index = sort.Search(len(elems), func(i int) bool {
		ret := elems[i].GetKey().Compare(key)
		return ret >= 0
	})
	return
}

func (elems BplusTreeElems) insert(elem BplusTreeElem, maxDegree int) (BplusTreeElems, error) {
	index := elems.find(elem.GetKey())
	log.Printf("found element's insert position at %d, (len: %d)\n", index, len(elems))
	// Insert at the end case.
	if index >= len(elems) {
		log.Printf("Appending %v at %d\n", elem, index)
		elems = append(elems, elem)
		return elems, nil
	}

	// If inserting in the middle.
	// XXX: Will need to use memMgr here.
	log.Printf("Inserting %v at %d, increasing elems to (size: %d, cap: %d)\n", elem, index, len(elems)+1, maxDegree+1)
	newElems := make(BplusTreeElems, len(elems)+1, maxDegree+1)
	copy(newElems, elems[:index])
	newElems[index] = elem
	copy(newElems[index+1:], elems[index:])
	return newElems, nil
}

func (elems BplusTreeElems) String() string {
	var elemStr []string
	for _, elem := range elems {
		switch elemType := elem.(type) {
		case *BplusTreeNode:
			elemStr = append(elemStr, fmt.Sprintf(" N <%v> ", elemType.GetKey()))
		default:
			elemStr = append(elemStr, fmt.Sprintf(" L <%v> ", elemType.GetKey()))
		}
	}
	return fmt.Sprintf("%v", elemStr)
}

// BplusTreeNode instance functions.
func (node *BplusTreeNode) insertElement(elem BplusTreeElem, maxDegree int) error {
	children, err := node.children.insert(elem, maxDegree)
	if err != nil {
		return err
	}

	node.children = children
	return nil
}

func (node *BplusTreeNode) GetKey() BplusTreeKey {
	var tmpNode *BplusTreeNode = node
	for !tmpNode.isLeaf {
		tmpNode = tmpNode.children[0].(*BplusTreeNode)
	}
	return tmpNode.children[0].GetKey()
}

func (node *BplusTreeNode) String() string {
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

// BplusTree instance functions.
func (bpt *BplusTree) BplusTreeNodeInit(
	node *BplusTreeNode,
	isLeaf bool,
	next *BplusTreeNode,
	prev *BplusTreeNode,
	initLen int) {

	node.children = make([]BplusTreeElem, initLen, bpt.context.maxDegree)
	node.isLeaf = isLeaf
	node.next = next
	node.prev = prev
}

func indexResetter(node *BplusTreeNode, index int) (int, error) {
	index -= 1
	if index <= 0 {
		index = 0
	}
	return index, nil
}

func (bpt *BplusTree) insertFinder(key BplusTreeKey) (nodes []*BplusTreeNode, err error) {
	return bpt.find(key, indexResetter)
}

func (bpt *BplusTree) find(key BplusTreeKey, resetter func(*BplusTreeNode, int) (int, error)) (nodes []*BplusTreeNode, err error) {
	nodes = make([]*BplusTreeNode, 0) // We don't know the capacity.
	node := bpt.root
	if node == nil {
		return nil, ERR_NOT_FOUND
	}

	for node != nil {
		nodes = append(nodes, node)
		if node.isLeaf {
			break
		}

		cnodes := node.children

		index := sort.Search(len(cnodes), func(i int) bool {
			ret := cnodes[i].GetKey().Compare(key)
			return ret >= 0 // Every key past this point is >=0
		})

		index, err = resetter(node, index)
		if err != nil {
			log.Printf("encountered err: %s\n", err)
			return
		}
		node = cnodes[index].(*BplusTreeNode)

	}
	return
}

func (bpt *BplusTree) searchFinder(key BplusTreeKey) (foundNode *BplusTreeNode, err error) {
	return bpt.searchInternal(key, indexResetter)
}

func (bpt *BplusTree) searchInternal(key BplusTreeKey, resetter func(*BplusTreeNode, int) (int, error)) (foundNode *BplusTreeNode, err error) {
	node := bpt.root
	err = nil
	foundNode = nil
	if node == nil {
		return nil, ERR_NOT_FOUND
	}

	for node != nil {
		cnodes := node.children
		if node.isLeaf {
			foundNode = node
			break
		}
		index := sort.Search(len(cnodes), func(i int) bool {
			ret := cnodes[i].GetKey().Compare(key)
			return ret >= 0 // Every key past this point is >=0
		})

		index, err = resetter(node, index)
		if err != nil {
			log.Printf("encountered err: %s\n", err)
			return
		}
		node = cnodes[index].(*BplusTreeNode)

	}
	return
}

func (bpt *BplusTree) rebalance(nodes []*BplusTreeNode) (err error) {
	numNodes := len(nodes)

	var parent, curr, next *BplusTreeNode

	switch {
	case numNodes == 1:
		curr = nodes[0]
		// XXX use memMgr.
		parent = BplusTreeDefaultAlloc()
		bpt.BplusTreeNodeInit(parent, false, nil, nil, 0)
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
	next = BplusTreeDefaultAlloc()
	bpt.BplusTreeNodeInit(next, curr.isLeaf, curr.next, curr, len(currChildren)-midp)
	curr.children = currChildren[:midp]
	copy(next.children, currChildren[midp:])
	curr.next = next

	if next.next != nil {
		next.next.prev = next
	}

	err = parent.insertElement(next, bpt.context.maxDegree)
	return
}

// Check to see if any rebalance is needed on the nodes traversed through insertion
// of a new element
func (bpt *BplusTree) checkRebalance(nodes []*BplusTreeNode) (err error) {
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

func (bpt *BplusTree) WriteTreeLayout(writer io.Writer) {
	leafIdx := 0
	nodeIdx := 0
	levelIdx := 0

	fmt.Fprintf(writer, "Dumping the tree layout.. \n")
	nodeList := bpt.root.children
	nodeLensList := make([]int, 1)
	nodeLensList[0] = len(bpt.root.children)
	numElems := nodeLensList[0]
	for i := 0; i < numElems; i++ {
		node := nodeList[i]
		switch elemType := node.(type) {
		case *BplusTreeNode:
			if elemType.isLeaf {
				fmt.Fprintf(writer, "<tree-L-node :%d, level: %d, node: %v> ",
					leafIdx, levelIdx, elemType)
				leafIdx += 1
			} else {
				fmt.Fprintf(writer, "<tree-I-node :%d, levelIdx: %d, node: %v> ",
					nodeIdx, levelIdx, elemType)
				nodeList = append(nodeList, elemType.children...)
				nodeLensList = append(nodeLensList, len(elemType.children))
				numElems += len(elemType.children)
			}
		default:
			fmt.Fprintf(writer, "<elem-node :%d, level: %d, node: %v> ",
				nodeIdx, levelIdx, elemType)
		}
		nodeIdx += 1
		if nodeIdx >= nodeLensList[levelIdx] {
			levelIdx += 1
			nodeIdx = 0
			fmt.Fprintf(writer, "\n")
		}
	}
	fmt.Fprintf(writer, "Done dumping the tree layout\n")
	fmt.Fprintf(writer, "----------------------------\n")
}
func (bpt *BplusTree) WriteTree(writer io.Writer, printLayout bool) error {
	node := bpt.root
	// Print tree layout.
	if printLayout == true {
		bpt.WriteTreeLayout(writer)
	}

	// Go to the left most leaf node and start printing in order.
	for node != nil {
		if node.isLeaf {
			break
		}
		node = node.children[0].(*BplusTreeNode)
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
		index += 1
	}
	return nil
}

func getKeysToLock(key BplusTreeKey) []BplusTreeKey {
	keys := make([]BplusTreeKey, 1)
	keys[0] = key
	return keys
}

//
// External functions.
//

// Create an instance of new tree. order and ctx specify the required parameters.
// Returns pointer to the tree if successful, appropriate error otherwise.
func NewBplusTree(ctx BplusTreeCtx) (*BplusTree, error) {
	log.Printf("Initializing new BplusTree...\n")

	if ctx.maxDegree < 0 {
		log.Printf("Invalid value for degree in the context.")
		return nil, ERR_INVALID_PARAM
	}

	// XXX: Use memory mgr to alloc.
	// XXX: Use dbMgr to load to initialize.
	bplustree := &BplusTree{root: nil, initialized: true, context: ctx}

	if BplusTreeIsEmptyInterface(ctx.lockMgr) {
		log.Printf("No locker specified. Using default locker using mutex\n")
		bplustree.context.lockMgr = new(BplusTreeDefaultLock)
		bplustree.context.lockMgr.Init()
	}

	return bplustree, nil
}

func (bpt *BplusTree) Insert(elem BplusTreeElem) error {
	if !bpt.initialized {
		return ERR_NOT_INITIALIZED
	}

	keys := getKeysToLock(elem.GetKey())
	bpt.context.lockMgr.Lock(keys, false)
	defer bpt.context.lockMgr.Unlock(keys, false)

	log.Printf("Inserting %v\n", elem)

	if bpt.root == nil {
		newnode := BplusTreeDefaultAlloc()
		bpt.BplusTreeNodeInit(newnode, true, nil, nil, 0)
		newnode.children = append(newnode.children, elem)
		bpt.root = newnode
		log.Printf("Done..... Inserting %v\n", elem)
		return nil
	}

	nodes, err := bpt.insertFinder(elem.GetKey())
	if err != nil {
		log.Printf("Inserting %v encountered error: %v\n", elem, err)
		return err
	}

	log.Printf("Accumulated following nodes\n")
	for _, n := range nodes {
		log.Printf("<%v> ", n)
	}
	log.Printf("\n----------------------------\n")

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

func (bpt *BplusTree) Remove(key BplusTreeKey) error {
	return nil
}

// Search in the tree. The 'ss' (BplusTreeSearchSpecifier) argument specifies what needs to be
// searched. Please look at BplusTreeSearchSpecifier for more details on specifying keys to
// search for.
func (bpt *BplusTree) Search(ss BplusTreeSearchSpecifier) (result []BplusTreeElem, err error) {
	keys := getKeysToLock(ss.searchKey)
	bpt.context.lockMgr.Lock(keys, true)
	defer bpt.context.lockMgr.Unlock(keys, true)

	if !bpt.initialized {
		log.Printf("Tree is not initialized\n")
		return nil, ERR_NOT_INITIALIZED
	}

	// XXX: Do some more sanity check for the input parameters.
	// What should be the max limit of # of elements? Using 50 for now.
	// Better would be to curtail the elems to 50? For now, return error.
	if ss.maxElems > 50 {
		log.Printf("maxElems too large\n")
		return nil, ERR_TOO_LARGE
	}

	// Get the leaf node where the element should be.
	node, err := bpt.searchFinder(ss.searchKey)
	if err != nil {
		log.Printf("Failed to find key: %v\n", ss.searchKey)
		return nil, err
	}

	// Do binary search in the leaf node.
	index := node.children.find(ss.searchKey)
	if index >= len(node.children) {
		// Special case when the key is the first element of a leaf in which
		// case the tree search will return the previous sibling always.
		if node.next != nil {
			node = node.next
			index = node.children.find(ss.searchKey)
		} else {
			log.Printf("Failed to find key: %v\n", ss.searchKey)
			return nil, ERR_NOT_FOUND
		}
	}

	matchingElem := node.children[index]

	if matchingElem == nil {
		log.Printf("Failed to find key: %v\n", ss.searchKey)
		return nil, ERR_NOT_FOUND
	}

	if ss.direction == Exact {
		result = make([]BplusTreeElem, 1)
		result[0] = matchingElem
		return result, nil
	}

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

	// No evaluator to be used.
	ignoreEvaluator := false
	if ss.evaluator == nil {
		ignoreEvaluator = true
	}
	evaluatorLeftExhausted := false
	evaluatorRightExhausted := false

	result = make([]BplusTreeElem, 1, ss.maxElems)
	result[0] = matchingElem

	leftStart := 0
	leftEnd := index
	leftNode := node
	leftIndex := leftEnd - 1
	if index == 0 {
		leftNode = node.prev
		if leftNode != nil {
			leftEnd = len(leftNode.children)
			leftIndex = len(leftNode.children) - 1
		}
	}

	rightStart := index + 1
	rightNode := node
	rightIndex := index + 1
	if index == len(node.children)-1 {
		rightNode = node.next
		if rightNode != nil {
			rightStart = 0
			rightIndex = 0
		}
	}

	if ignoreEvaluator {
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
		for numElemsLeft > 0 && leftNode != nil {
			elemToLeft := leftNode.children[leftIndex]
			evaluatorLeftExhausted = !ss.evaluator(ss.searchKey, elemToLeft.GetKey())
			if !evaluatorLeftExhausted {
				result = append([]BplusTreeElem{elemToLeft}, result...)
				leftIndex--
				numElemsLeft--
				if leftIndex < 0 {
					leftNode = leftNode.prev
					if leftNode != nil {
						leftIndex = len(leftNode.children) - 1
					}
				}
			} else {
				break
			}

		}
		for numElemsRight > 0 && rightNode != nil {
			elemToRight := rightNode.children[rightIndex]
			evaluatorRightExhausted = !ss.evaluator(ss.searchKey, elemToRight.GetKey())
			if !evaluatorRightExhausted {
				result = append(result, elemToRight)
				rightIndex++
				numElemsRight--
				if rightIndex >= len(rightNode.children) {
					rightNode = rightNode.next
					if rightNode != nil {
						rightIndex = 0
					}
				}
			} else {
				break
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

func (bpt *BplusTree) Print() error {
	return bpt.WriteTree(os.Stdout, true)
}
