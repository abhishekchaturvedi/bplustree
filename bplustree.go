// Use of this software is governed by an Apache 2.0
// licence which can be found in the license file

// This file implements a persistent B+ Tree in golang. The tree uses some
// interfaces for locking, memory management and persistence on disk.

package bplustree

import (
	"bplustree/common"
	"sort"

	"github.com/golang/glog"
)

// opTracker -- this struct maintains tracking of nodes which are getting
// created, updated and the original copy when a tree operation is performed.
// This information is only live/valid during a given tree operation and is
// discarded after the operation is complete. This information helps in rollback
// if needed.
// rootKeyUpdated- Whether the root key is updated.
// origRootKey  - original root key (before the operation)
// origNodes    - map of nodes with their original values before they are
//                modified. Helps in maintaining COW versions.
// updatedNodes - Nodes which are updated during a tree op.
// newNodes     - Nodes which are newly created during a tree op.
// deletedNodes - Nodes which are deleted during a tree op.
type opTracker struct {
	rootKeyUpdated bool
	origRootKey    common.Key
	origNodes      Tracker
	updatedNodes   Tracker
	newNodes       Tracker
	deletedNodes   Tracker
}

// Context - This struct defines the optional and necessary user-defined
// functions which will be used for certain operations.
// 'lockMgr' is the optional interface for user-defined lock/unlock fuctions.
// 'dbMgr' is the optional interface to define interaction with the persistence
//         layer. The details of the interface are defined in DBMgr interface.
// 'memMgr' is the interface to define the memory manager. See MemMgr interface
//         for details.
// 'maxDegree' is the maximum degree of the tree.
// 'keyType' is the key type that's used for the internal node keys of the tree.
//          This is not the same as the user's key. More details in key.go
// 'pfx' is the prefix to be used in the key name if string type.
type Context struct {
	lockMgr   Locker
	dbMgr     DBMgr
	memMgr    MemMgr
	maxDegree int
	keyType   common.KeyType
	pfx       string
}

// Base -- is the base information that needs persisting.
// 'RootKey' -- is the root key of the tree.
// 'Degree'  -- is the degree of the tree.
type Base struct {
	RootKey common.Key `json:"rootkey"`
	Degree  int        `json:"degree"`
}

// BplusTree - The tree itself. Contains the root and some context information.
// root of the tree. Is a node itself.
// 'context' is the context which defines interfaces used for various aspects
//           as defined by 'Context' interface.
// 'initialized' indicates whether the tree has been initialized or not.
type BplusTree struct {
	rootKey     common.Key
	context     Context
	initialized bool
	tracker     opTracker
}

// Element - user-defined data/content of the node.
// Each element is exepcted to be a serialized buffer containing the size and
// the data as byte array.
type Element struct {
	Buffer [256]byte `json:"buffer"`
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
	evaluator(key common.Key) bool
}

// SearchSpecifier - Contains user defined policy for how the
// search of a given key is to be conducted.
// searchKey specifies the exact key for which the search needs to be done.
// direction specifies the direction of the search. It could be exact, left,
// right or both. maxElems specifies the maximum number of elements that will
// be returned by the search matching the search criteria. This argument is
// ignored when using 'Exact' direction. evaluator defines the comparison
// function to be used to while traversing keys to the left or right of the
// searchKey. This is arugment is ignored when using 'Exact' direction.
// For example:
// ss BplusTreeSearchSpecifier := {'foo', Exact, 10, MyEvalFn}
// will search for 'foo' key with exact match.
// ss BplusTreeSearchSpecifier := {'foo', Left, 10, nil}
// will search for 'foo' and return 10 keys to the left of 'foo'.
// ss BplusTreeSearchSpecifier := {'foo', Both, 10, nil}
// will search for 'foo' and return 10 keys total, 5 to the Left of 'foo' and
// 5 to the right of 'foo'.
// ss BplusTreeSearchSpecifier := {'foo', Both, 10, MyEvalFunc}
// will search for 'foo' and return maximum of 10 keys total, 5 to the Left
// of 'foo' and 5 to the right of 'foo' as long as each of those keys when
// evaluated using the 'evaluator' returns 'true'. First argument of the
// 'evaluator' function will always be the input key itself and the second
// argument will be the key with which it is being compared.
type SearchSpecifier struct {
	searchKey common.Key
	direction SearchDirection
	maxElems  int
	evaluator func(common.Key, common.Key) bool
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
func getKeysToLock(key common.Key) []common.Key {
	keys := make([]common.Key, 1)
	keys[0] = key
	return keys
}

//
// Section - BplusTree instance functions.
//

// initTracker - initialize/reset the tracker.
func (bpt *BplusTree) initTracker() {
	bpt.tracker.newNodes.Init("new", false)
	bpt.tracker.updatedNodes.Init("updated", false)
	bpt.tracker.origNodes.Init("orig", true)
	bpt.tracker.deletedNodes.Init("deleted", false)
	bpt.tracker.origRootKey = bpt.rootKey
	bpt.tracker.rootKeyUpdated = false
}

// resetTracker - reset all trackers.
func (bpt *BplusTree) resetTracker() {
	bpt.tracker.newNodes.Reset()
	bpt.tracker.updatedNodes.Reset()
	bpt.tracker.origNodes.Reset()
	bpt.tracker.deletedNodes.Reset()
	bpt.tracker.origRootKey = common.Key{BPTkey: ""}
}

// fetch - fetch key from memMgr. If not found, then the key is loaded from
// db and also inserted in the memory manager to cache it. It's possible that
// we are fetching a key during rebalance/merge on the way back in an operation
// in which case, the key may not have yet been added to memory manager or DB.
// Let's always start the fetch from newly added or updated nodes.
func (bpt *BplusTree) fetch(k common.Key) (*treeNode, error) {
	if k.IsNil() {
		glog.Warningf("key is nil")
		return nil, nil
	}

	if (len(bpt.tracker.newNodes.kmap)) != 0 {
		e, ok := bpt.tracker.newNodes.kmap[k]
		if ok {
			return e.(*treeNode), nil
		}
		glog.V(2).Infof("did not find %v in new nodes, look in memmgr", k)
	}

	node, err := bpt.context.memMgr.Lookup(k)
	if err == nil {
		return node.(*treeNode), nil
	}

	glog.V(1).Infof("mem lookup failed with %v for %v, getting from db.", err, k)
	if TestPointExecute(testPointFailDBFetch) {
		return nil, common.ErrDBLoadFailed
	}

	node, err = bpt.context.dbMgr.Load(k)
	if err != nil {
		glog.Errorf("db load returned error %v for key %v", err, k)
		return nil, err
	}

	_ = bpt.context.memMgr.Insert(k, node) // Memory insert can't fail.
	return node.(*treeNode), nil
}

// rollback -- rollback the updated in memory treenode values with the original
// ones. This operation is needed whenever we have encountered an error midway
// in a tree operation. This is all in-memory operation and can't fail.
func (bpt *BplusTree) rollback() {
	glog.V(2).Infof("Rolling back")
	for k, e := range bpt.tracker.origNodes.kmap {
		_ = bpt.context.memMgr.Insert(k, e)
	}
	// if root key is updated, then revert.
	if bpt.tracker.rootKeyUpdated {
		bpt.rootKey = bpt.tracker.origRootKey
	}
}

// Update the memory manager and the db with the changes made during the
// tree op that's concluding. Handle errors with complete atomicity.
// opErr -- is the error code of the operation on the tree. If opErr is nil,
// then we need to commit the changes, otherwise we have to rollback to begin
// with.
// Returns error if update fails.
func (bpt *BplusTree) update(opErr error) error {
	var err error = nil
	defer bpt.resetTracker()

	glog.V(2).Infof("updating with following trakcers (err: %v)", opErr)
	glog.V(2).Infof("orig: %v", bpt.tracker.origNodes)
	glog.V(2).Infof("updated: %v", bpt.tracker.updatedNodes)
	glog.V(2).Infof("new: %v", bpt.tracker.newNodes)
	glog.V(2).Infof("deleted: %v", bpt.tracker.deletedNodes)
	// if the operation so far has resulted in error, then just roll back the
	// updates on 'updateList' with values in 'origList'.
	if opErr != nil {
		bpt.rollback()
		return opErr
	}

	numUpdates := len(bpt.tracker.updatedNodes.kmap) + len(bpt.tracker.newNodes.kmap) +
		len(bpt.tracker.deletedNodes.kmap)
	if bpt.tracker.rootKeyUpdated {
		numUpdates++
	}
	dbUpdates := make([]common.DBOp, numUpdates)
	index := 0
	for k, e := range bpt.tracker.updatedNodes.kmap {
		glog.V(2).Infof("Adding %v (%v) to store list", k, e)
		dbUpdates[index] = common.DBOp{Op: common.DBOpStore, K: k, E: e}
		index++
	}
	for k, e := range bpt.tracker.newNodes.kmap {
		glog.V(2).Infof("Adding %v (%v) to store list", k, e)
		dbUpdates[index] = common.DBOp{Op: common.DBOpStore, K: k, E: e}
		index++
	}
	for k := range bpt.tracker.deletedNodes.kmap {
		glog.V(2).Infof("Adding %v to delete list", k)
		dbUpdates[index] = common.DBOp{Op: common.DBOpDelete, K: k, E: nil}
		index++
	}
	if bpt.tracker.rootKeyUpdated {
		dbUpdates[index] = common.DBOp{Op: common.DBOpSetRoot, K: common.Key{},
			E: &Base{bpt.rootKey, bpt.context.maxDegree}}
		index++
	}

	if numUpdates > 0 {
		if TestPointExecute(testPointFailDBUpdate) {
			err = common.ErrDBUpdateFailed
		} else {
			err = bpt.context.dbMgr.AtomicUpdate(dbUpdates)
		}
		if err != nil {
			glog.Errorf("falied to store keys %v to DB (err: %v)", dbUpdates, err)
			bpt.rollback()
			return err
		}
	}

	// If we are here then update to DB is successful. We just need to now
	// insert all the new nodes in memory manager.
	for k, e := range bpt.tracker.newNodes.kmap {
		_ = bpt.context.memMgr.Insert(k, e)
	}
	// Remove deleted nodes from mem manager.
	for k := range bpt.tracker.deletedNodes.kmap {
		_ = bpt.context.memMgr.Remove(k)
	}

	return opErr
}

// treeNodeInit - initializer for a given treeNode
func (bpt *BplusTree) treeNodeInit(isLeaf bool, next common.Key, prev common.Key,
	initLen int) *treeNode {

	node := defaultAlloc()
	node.Children = make([]treeNodeElem, initLen, bpt.context.maxDegree)
	node.IsLeaf = isLeaf
	node.NextKey = next
	node.PrevKey = prev
	// Generate a new key for the node being added.
	node.NodeKey = common.Generate(bpt.context.keyType, bpt.context.pfx)
	return node
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

// find - find a given key in the BplusTree. The function gathers the list of
// treeNodes found in the path to the leaf node which is equal or lesser than
// the provided key.
// Returns the list of nodes gathered on the path, and error if any.
func (bpt *BplusTree) find(key common.Key, op treeOpType) ([]*treeNode, []int, error) {
	var indexes []int
	nodes := make([]*treeNode, 0) // We don't know the capacity.

	node, err := bpt.fetch(bpt.rootKey)
	if node == nil {
		return nil, nil, err
	}

	for node != nil {
		if node.IsLeaf {
			nodes = append(nodes, node)
			break
		}
		// Only accumulate nodes if this is find for 'insert' or 'remove'.
		// For search, we are only interested in the leaf node.
		if op == treeOpInsert || op == treeOpRemove {
			nodes = append(nodes, node)
		}

		cnodes := node.Children
		exactMatch := false
		index := sort.Search(len(cnodes), func(i int) bool {
			ret := cnodes[i].DataKey.Compare(key)
			if ret == 0 {
				exactMatch = true
			}
			return ret >= 0 // Every key past this point is >=0
		})

		index = indexResetter(index, op, exactMatch)
		if op == treeOpRemove || op == treeOpInsert {
			indexes = append(indexes, index)
		}
		node, err = bpt.fetch(cnodes[index].NodeKey)
		if err != nil {
			glog.Errorf("failed to fetch %v (err: %v)", cnodes[index].NodeKey, err)
			return nil, nil, err
		}
	}
	return nodes, indexes, err
}

// insertFinder - Function to find the path of nodes in the BplusTree where the
// given key needs to be inserted.
// Retuns the list of nodes which are encoutered on the path to the leaf node
// which is smaller in order to the specified key.
func (bpt *BplusTree) insertFinder(key common.Key) ([]*treeNode, []int, error) {
	return bpt.find(key, treeOpInsert)
}

// searchFinder - This function looks for the specified key and returns the
// leaf node which contains the key
func (bpt *BplusTree) searchFinder(key common.Key) ([]*treeNode, []int, error) {
	return bpt.find(key, treeOpSearch)
}

// removeFinder - This function looks for the specified key and returns all
// nodes on the path to the leaf node which contains the key as well as the
// indexes in the parent node to the path leading to the leaf node.
func (bpt *BplusTree) removeFinder(key common.Key) ([]*treeNode, []int, error) {
	return bpt.find(key, treeOpRemove)
}

// rebalance - rebalances the tree once a new node is inserted.
// returns error if any
func (bpt *BplusTree) rebalance(nodes []*treeNode, indexes []int) error {
	var err error
	var parent, curr, next *treeNode
	numNodes := len(nodes)
	numIndexes := len(indexes)
	var cIndex int
	pCreated := false

	curr = nodes[numNodes-1]
	// Add the current node's original value to the map.
	bpt.tracker.origNodes.AddIfNotPresent(curr.NodeKey, curr)

	switch {
	case numNodes == 1:
		parent = bpt.treeNodeInit(false, common.Key{BPTkey: ""}, common.Key{BPTkey: ""}, 0)
		tneCurr := &treeNodeElem{NodeKey: curr.NodeKey, DataKey: curr.DataKey}
		parent.Children = append(parent.Children, *tneCurr)
		// Make the new node the parent, and everything else accumulated
		// so far becomes the children.
		bpt.tracker.origRootKey = bpt.rootKey
		bpt.tracker.rootKeyUpdated = true
		bpt.rootKey = parent.NodeKey
		cIndex = 0
		pCreated = true
	default:
		parent = nodes[numNodes-2]
		cIndex = indexes[numIndexes-1]
		bpt.tracker.origNodes.AddIfNotPresent(parent.NodeKey, parent)
	}

	currChildren := curr.Children
	midp := len(currChildren) / 2

	next = bpt.treeNodeInit(curr.IsLeaf, curr.NextKey, curr.NodeKey,
		len(currChildren)-midp)
	curr.Children = currChildren[:midp]
	copy(next.Children, currChildren[midp:])
	curr.NextKey = next.NodeKey
	next.updateDataKey(nil, -1, nil)
	// curr updated and next created. Add to trackers.
	bpt.tracker.newNodes.AddIfNotPresent(next.NodeKey, next)
	bpt.tracker.updatedNodes.AddIfNotPresent(curr.NodeKey, curr)
	glog.V(2).Infof("curr: %v", curr)
	glog.V(2).Infof("next: %v", next)
	if !next.NextKey.IsNil() {
		nextnext, err := bpt.fetch(next.NextKey)
		if nextnext == nil {
			glog.Errorf("failed to fetch %v (err: %v)", next.NextKey, err)
			return err
		}
		bpt.tracker.origNodes.AddIfNotPresent(nextnext.NodeKey, nextnext)
		nextnext.PrevKey = next.NodeKey
		bpt.tracker.updatedNodes.AddIfNotPresent(nextnext.NodeKey, nextnext)
	}
	tneNext := &treeNodeElem{NodeKey: next.NodeKey, DataKey: next.DataKey}
	parent.insertElement(tneNext, bpt.context.maxDegree)
	// Parent updated or created. Add to trackers.
	if pCreated {
		bpt.tracker.newNodes.AddIfNotPresent(parent.NodeKey, parent)
	} else {
		bpt.tracker.updatedNodes.AddIfNotPresent(parent.NodeKey, parent)
	}
	_, err = parent.updateDataKey(bpt, cIndex, nil)
	if err != nil {
		glog.Errorf("failed to updated parent %v (err: %v)", parent, err)
		return err
	}
	glog.V(2).Infof("parent: %v", parent)
	return err
}

// checkRebalance - Check to see if any rebalance is needed on the nodes
// traversed through insertion of a new element.
// Returns error if encountered.
func (bpt *BplusTree) checkRebalance(nodes []*treeNode, indexes []int) error {
	var err error
	// Traverse in reverse order to address the nodes towards leaves first.
	for i := len(nodes) - 1; i >= 0; i-- {
		node := nodes[i]
		degree := bpt.context.maxDegree

		if len(node.Children) > degree {
			glog.V(2).Infof("degree %d (allowed: %d) node %v found, splitting",
				len(node.Children), degree, node)
			err = bpt.rebalance(nodes[:i+1], indexes[:i])
			if err != nil {
				glog.Errorf("failed to rebalance: %v\n", err)
				break
			}
		} else {
			glog.V(2).Infof("node %v (leaf: %v) has degree (%d). Allowed",
				node, node.IsLeaf, len(node.Children))
			// even if no rebalance is required, the dataKey may need to
			// be updated since a new entry is added to the children.
			// No need to do this for the leaf node, because when the leaf
			// node is inserted, we have already taken care of that.
			if i != len(nodes)-1 {
				bpt.tracker.origNodes.AddIfNotPresent(node.NodeKey, node)
				_, err = node.updateDataKey(bpt, indexes[i], nil)
				if err != nil {
					glog.Errorf("failed to update node %v (err: %v)", node, err)
					break
				}
				bpt.tracker.updatedNodes.AddIfNotPresent(node.NodeKey, node)
			}
		}
	}
	return err
}

// merge - merge siblings as needed after a node is removed.
func (bpt *BplusTree) merge(parent *treeNode, curr *treeNode, currNodeIdx int,
	sibling *treeNode, siblIndex int, direction SearchDirection) error {

	// If merging with right sibling then append right sibling's chilren
	// to current node, otherwise append current nodes' children to left
	// sibling.
	var cIndex int

	// save all nodes before modifying.
	bpt.tracker.origNodes.AddIfNotPresent(curr.NodeKey, curr)
	if sibling != nil {
		bpt.tracker.origNodes.AddIfNotPresent(sibling.NodeKey, sibling)
	}
	bpt.tracker.origNodes.AddIfNotPresent(parent.NodeKey, parent)
	switch {
	case direction == Right:
		glog.V(2).Infof("merging %v and right sibling %v\n", curr, sibling)
		curr.Children = append(curr.Children, sibling.Children...)
		curr.NextKey = sibling.NextKey
		// current is modified, add that to updatedNodes tracker.
		bpt.tracker.updatedNodes.AddIfNotPresent(curr.NodeKey, curr)
		if !sibling.NextKey.IsNil() {
			sibnext, err := bpt.fetch(sibling.NextKey)
			if err != nil {
				glog.Errorf("failed to fetch %v during merge (err: %v)",
					sibling.NextKey, err)
				return err
			}
			bpt.tracker.origNodes.AddIfNotPresent(sibnext.NodeKey, sibnext)
			sibnext.PrevKey = curr.NodeKey
			bpt.tracker.updatedNodes.AddIfNotPresent(sibnext.NodeKey, sibnext)
		}
		// sibling is being dropped, add that to deletedNodes tracker.
		bpt.tracker.deletedNodes.AddIfNotPresent(sibling.NodeKey, sibling)
		// drop the entry of the right node from parent's children.
		parent.Children = append(parent.Children[:siblIndex],
			parent.Children[siblIndex+1:]...)
		cIndex = currNodeIdx
	case direction == Left:
		glog.V(2).Infof("merging left sibling %v and %v\n", sibling, curr)
		sibling.Children = append(sibling.Children, curr.Children...)
		sibling.NextKey = curr.NextKey
		// sibling is modified, add that to updatedNodes tracker.
		bpt.tracker.updatedNodes.AddIfNotPresent(sibling.NodeKey, sibling)
		if !curr.NextKey.IsNil() {
			currnext, err := bpt.fetch(curr.NextKey)
			if err != nil {
				glog.Errorf("failed to fetch %v during merge (err: %v)",
					curr.NextKey, err)
				return err
			}
			bpt.tracker.origNodes.AddIfNotPresent(currnext.NodeKey, currnext)
			currnext.PrevKey = sibling.NodeKey
			bpt.tracker.updatedNodes.AddIfNotPresent(currnext.NodeKey, currnext)
		}
		// current node is being dropped, add that to deletedNodes tracker.
		bpt.tracker.deletedNodes.AddIfNotPresent(curr.NodeKey, curr)
		// drop the entry of the current node from parent's children.
		// Note that if current node is rightmost node, then we need to handle
		// that specially.
		if siblIndex == (len(parent.Children) - 2) {
			parent.Children = parent.Children[:siblIndex+1]
		} else {
			parent.Children = append(parent.Children[:siblIndex+1],
				parent.Children[siblIndex+2:]...)
		}
		cIndex = siblIndex
	default: // Merging with parent.
		glog.V(2).Infof("merging Parent %v and %v\n", parent, curr)
		// curr is being dropped, add that to deletedNodes tracker.
		bpt.tracker.deletedNodes.AddIfNotPresent(curr.NodeKey, curr)
		parent.Children = curr.Children
		parent.IsLeaf = curr.IsLeaf
	}

	// Parent's children has been modified. update tracker and dataKey
	bpt.tracker.updatedNodes.AddIfNotPresent(parent.NodeKey, parent)
	_, err := parent.updateDataKey(bpt, cIndex, nil)
	return err
}

// distribute - redistribute nodes among siblings for balancing the tree when
// a node is removed. This function will make the # of children equal (off by
// 1 max) in the current node and the sibling node.
func (bpt *BplusTree) distribute(parent *treeNode, curr *treeNode, currNodeIdx int,
	sibling *treeNode, siblIndex int, direction SearchDirection) error {

	// Distribute the children from sibling to the current node such
	// that both of them have equal lengths.
	numChildrenToDistribute := (len(sibling.Children) -
		(len(sibling.Children)+len(curr.Children))/2)

	// Save original values of all nodes before modifying.
	bpt.tracker.origNodes.AddIfNotPresent(curr.NodeKey, curr)
	bpt.tracker.origNodes.AddIfNotPresent(sibling.NodeKey, sibling)
	bpt.tracker.origNodes.AddIfNotPresent(parent.NodeKey, parent)
	switch {
	case direction == Right:
		glog.V(2).Infof("distributing %d elements from right sibling %v and %v\n",
			numChildrenToDistribute, sibling, curr)
		curr.Children = append(curr.Children,
			sibling.Children[:numChildrenToDistribute]...)
		sibling.Children = sibling.Children[numChildrenToDistribute:]
	case direction == Left:
		glog.V(2).Infof("distributing %d elements from left sibling %v and %v\n",
			numChildrenToDistribute, sibling, curr)
		nLeft := len(sibling.Children)
		tmpChildren := sibling.Children[nLeft-numChildrenToDistribute:]
		sibling.Children = sibling.Children[:nLeft-numChildrenToDistribute]
		curr.Children = append(tmpChildren, curr.Children...)
	default:
		panic("unexpected condition")
	}

	bpt.tracker.updatedNodes.AddIfNotPresent(sibling.NodeKey, sibling)
	bpt.tracker.updatedNodes.AddIfNotPresent(curr.NodeKey, curr)

	// children for both the current node and the sibliing is modified.
	// the dataKey therefore needs updating.
	_, err := curr.updateDataKey(nil, -1, nil)
	if err != nil {
		glog.Errorf("failed to update %v during distribute (err: %v)",
			curr, err)
		return err
	}

	_, err = sibling.updateDataKey(nil, -1, nil)
	if err != nil {
		glog.Errorf("failed to update %v during distribute (err: %v)",
			curr, err)
		return err
	}
	bpt.tracker.updatedNodes.AddIfNotPresent(parent.NodeKey, parent)
	parent.Children[siblIndex].DataKey = sibling.DataKey
	parent.Children[currNodeIdx].DataKey = curr.DataKey
	return nil
}

// checkMerge - Check to see if any merging is needed on the nodes traversed
// through deletion after an element is deleted. Following are the scenarios
// where we'd need redistributing or merging. If the current node's # of
// children is less than 1/2 of allowed degree (d), and nLeft and nRight be the
// # of children that the left and right sibling nodes have, then:
// nLeft        nRight      Action
// nil			nil			Merge with parent
// nil			<=d/2		Merge with right sibling
// nil			>d/2		Distribute with right sibling
// <=d/2		nil			Merge with left sibling
// <=d/2		<d/2		Merge with sibling which has less children
// <=d/2		>d/2		Merge with left sibling
// >d/2			nil			Distribute with left sibling
// >d/2			<=d/2		Merge with right sibling
// >d/2			>d/2		Distribute with sibling whichever has less children
// Returns error if encountered.
func (bpt *BplusTree) checkMergeOrDistribute(nodes []*treeNode, indexes []int) error {

	// Traverse in reverse order to address the nodes towards leaves first.
	// Note the iteration to 1, as nothing to be done for the root node if the
	// degree drops below d/2
	for i := len(nodes) - 1; i >= 1; i-- {
		node := nodes[i]
		maxDegree := bpt.context.maxDegree
		minDegree := maxDegree/2 + 1
		if len(node.Children) >= minDegree {
			if i != len(nodes)-1 {
				bpt.tracker.origNodes.AddIfNotPresent(node.NodeKey, node)
				updated, _ := node.updateDataKey(bpt, indexes[i], nil)
				if updated {
					bpt.tracker.updatedNodes.AddIfNotPresent(node.NodeKey, node)
				}
			}
			continue
		}

		glog.V(2).Infof("found degree %d (allowed: %d) node %v. merging/redistributing\n",
			len(node.Children), minDegree, node)

		parent := nodes[i-1]
		var leftSibl, rightSibl *treeNode = nil, nil
		var nLeft, nRight int = 0, 0
		var lIndex, rIndex int = 0, 0
		var err error = nil
		cIndex := indexes[i-1]
		if cIndex != 0 {
			lIndex = cIndex - 1
			leftSibl, err = bpt.fetch(parent.Children[lIndex].NodeKey)
			if leftSibl == nil {
				glog.Errorf("failed to fetch %v (err: %v)",
					parent.Children[lIndex].NodeKey, err)
				return err
			}
			nLeft = len(leftSibl.Children)
		}
		if cIndex < (len(parent.Children) - 1) {
			rIndex = cIndex + 1
			rightSibl, err = bpt.fetch(parent.Children[rIndex].NodeKey)
			if rightSibl == nil {
				glog.Errorf("failed to fetch %v (err: %v)",
					parent.Children[rIndex].NodeKey, err)
				return err
			}
			nRight = len(rightSibl.Children)
		}
		glog.V(2).Infof("i: %d, indexes: %v, parent: %v\n",
			i, indexes, parent)
		glog.V(2).Infof("lIndex: %d, nLeft: %d, leftSibl: %v\n",
			lIndex, nLeft, leftSibl)
		glog.V(2).Infof("rIndex: %d, nRight: %d, rightSibl: %v\n",
			rIndex, nRight, rightSibl)
		switch {
		case nLeft == 0 && nRight == 0:
			err = bpt.merge(parent, node, 0, nil, 0, Exact)
		case nLeft == 0 && nRight <= minDegree:
			err = bpt.merge(parent, node, cIndex, rightSibl, rIndex, Right)
		case nLeft == 0 && nRight > minDegree:
			err = bpt.distribute(parent, node, cIndex, rightSibl, rIndex, Right)
		case nLeft <= minDegree && nRight == 0:
			err = bpt.merge(parent, node, cIndex, leftSibl, lIndex, Left)
		case nLeft <= minDegree && nRight <= minDegree:
			sibl := leftSibl
			direction := SearchDirection(Left)
			ind := lIndex
			if nRight < nLeft {
				sibl = rightSibl
				direction = Right
				ind = rIndex
			}
			err = bpt.merge(parent, node, cIndex, sibl, ind, direction)
		case nLeft <= minDegree && nRight > minDegree:
			err = bpt.merge(parent, node, cIndex, leftSibl, lIndex, Left)
		case nLeft > minDegree && nRight == 0:
			err = bpt.distribute(parent, node, cIndex, leftSibl, lIndex, Left)
		case nLeft > minDegree && nRight <= minDegree:
			err = bpt.merge(parent, node, cIndex, rightSibl, rIndex, Right)
		case nLeft > minDegree && nRight > minDegree:
			sibl := leftSibl
			direction := SearchDirection(Left)
			ind := lIndex
			if nRight < nLeft {
				sibl = rightSibl
				direction = Right
				ind = rIndex
			}
			err = bpt.distribute(parent, node, cIndex, sibl, ind, direction)
		}
		if err != nil {
			glog.Errorf("failed to merge or distribute: %v", err)
			return err
		}
	}
	return nil
}

// writeLayout - Writes the tree layout on the provided writer
func (bpt *BplusTree) writeLayout() {
	leafIdx := 0
	nodeIdx := 0
	levelIdx := 0

	if !bpt.initialized || bpt.rootKey.IsNil() {
		return
	}

	rootNode, _ := bpt.fetch(bpt.rootKey)
	if rootNode == nil {
		glog.Errorf("failed to fetch root key: %v. can not print the tree.",
			bpt.rootKey)
		return
	}
	glog.Infof("dumping the tree layout.. numChildren: %d\n",
		len(rootNode.Children))
	nodeList := rootNode.Children
	nodeLensList := make([]int, 1)
	nodeLensList[0] = len(rootNode.Children)
	numElems := nodeLensList[0]
	numNodesAtLevel := 0
	printLevel := true
	glog.Infof("level -- 0    <root: %v>\n", rootNode)
	if rootNode.IsLeaf {
		return
	}
	for i := 0; i < numElems; i++ {
		if printLevel {
			glog.Infof("level -- %d    ", levelIdx+1)
			printLevel = false
		}
		node, _ := bpt.fetch(nodeList[i].NodeKey)
		if node == nil {
			glog.Errorf("failed to fetch root key: %v", nodeList[i].NodeKey)
			return
		}

		if node.IsLeaf {
			glog.Infof("level:%d <tree-L-node :%d, node: %v> ", levelIdx+1, leafIdx, node)
			leafIdx++
		} else {
			glog.Infof("level:%d <tree-I-node :%d, node: %v> ", levelIdx+1, nodeIdx, node)
			nodeList = append(nodeList, node.Children...)
			numElems += len(node.Children)
			numNodesAtLevel += len(node.Children)
		}
		nodeIdx++
		if nodeIdx >= nodeLensList[levelIdx] {
			nodeLensList = append(nodeLensList, numNodesAtLevel)
			levelIdx++
			nodeIdx = 0
			numNodesAtLevel = 0
			glog.Infof("\n")
			printLevel = true
		}
	}
	glog.Infof("done.. dumping the layout\n")
	glog.Infof("----------------------------\n")
}

// writeTree - writes the tree (including the layout if requested) to the
// provided IO writer.
func (bpt *BplusTree) writeTree(printLayout bool) {
	defer glog.Flush()
	node, _ := bpt.fetch(bpt.rootKey)
	if node == nil {
		glog.Errorf("failed to fetch root key: %v", bpt.rootKey)
		return
	}
	// Print tree layout.
	if printLayout == true {
		bpt.writeLayout()
	}

	// Go to the left most leaf node and start printing in order.
	for node != nil {
		if node.IsLeaf {
			break
		}
		node, _ = bpt.fetch(node.Children[0].NodeKey)
		if node == nil {
			glog.Errorf("failed to fetch key: %v", node.Children[0].NodeKey)
			return
		}
	}

	if node == nil {
		glog.Infof("tree is empty")
		return
	}

	index := 0
	for {
		glog.Infof("leaf node: %d (DK: %v, NK: %v, XK: %v, PK: %v)\n",
			index, node.DataKey, node.NodeKey, node.NextKey, node.PrevKey)
		for _, child := range node.Children {
			glog.Infof("\t%v\n", child)
		}

		if node.NextKey.IsNil() {
			break
		}

		if !node.NextKey.IsNil() {
			nextKey := node.NextKey
			node, _ = bpt.fetch(nextKey)
			if node == nil {
				glog.Errorf("failed to fetch key: %v", nextKey)
				break
			}
		}
		index++
	}
}

// accumulate -- Accumulate tall the elements from 'start' to 'end' in
// a leaf node.
func (bpt *BplusTree) accumulate(node *treeNode, start int, end int) []Element {
	if end <= start {
		glog.Errorf("invalid end (%d) and start (%d)", end, start)
		return nil
	}
	result := make([]Element, end-start)
	for i := start; i < end; i++ {
		result[i-start] = node.Children[i].Data
	}
	return result
}

//
// External functions.
//

// NewBplusTree - Create an instance of new tree. order and ctx specify the
// required parameters.
// Since this is a persistent BplusTree implementation, some dbMgr implementation
// is necessary to be provided in the context.
// If a valid root base is found then the tree is instantiated using that base
// otherwise a new tree instance is created.
// If paramter validation fail then appropriate error otherwise.
func NewBplusTree(ctx Context) (*BplusTree, error) {
	if ctx.maxDegree < 3 {
		glog.Errorf("invalid value for degree in the context.")
		return nil, common.ErrInvalidParam
	}

	if isEmptyInterface(ctx.dbMgr) {
		glog.Infof("no db manager specified. Can't move forward\n")
		return nil, common.ErrInvalidParam
	}

	bplustree := &BplusTree{rootKey: common.Key{BPTkey: ""}, initialized: true, context: ctx}

	base, err := bplustree.context.dbMgr.GetRoot()
	if err != nil {
		glog.Errorf("Fail to load the root key: (err: %v)", err)
		return nil, err
	}
	if base != nil {
		bplustree.rootKey = base.RootKey
		bplustree.context.maxDegree = base.Degree
	}

	if isEmptyInterface(ctx.lockMgr) {
		glog.Infof("no locker specified. Using default locker using mutex\n")
		bplustree.context.lockMgr = new(defaultLock)
		bplustree.context.lockMgr.Init()
	}

	return bplustree, nil
}

// Insert - Insert an element to the tree. The 'elem' specified needs to
// extend the 'Element' interface.
func (bpt *BplusTree) Insert(key common.Key, elem Element) error {
	if !bpt.initialized {
		return common.ErrNotInitialized
	}

	keys := getKeysToLock(key)
	bpt.context.lockMgr.Lock(nil, keys)
	defer bpt.context.lockMgr.Unlock(nil, keys)

	bpt.initTracker()
	glog.Infof("inserting: %v:%v\n", key, elem)
	if glog.V(2) {
		defer glog.Flush()
	}

	if bpt.rootKey.IsNil() {
		newnode := bpt.treeNodeInit(true, common.Key{BPTkey: ""}, common.Key{BPTkey: ""}, 0)
		newnode.DataKey = key
		tne := &treeNodeElem{NodeKey: common.Key{BPTkey: ""}, DataKey: key, Data: elem}
		newnode.Children = append(newnode.Children, *tne)
		bpt.rootKey = newnode.NodeKey
		bpt.tracker.rootKeyUpdated = true
		bpt.tracker.newNodes.AddIfNotPresent(newnode.NodeKey, newnode)
		err := bpt.update(nil)
		if err != nil {
			glog.Errorf("inserting: %v:%v failed (err: %v)", key, elem, err)
			return err
		}
		glog.Infof("done..... inserting: %v:%v (success)\n", key, elem)
		return nil
	}

	nodes, indexes, err := bpt.insertFinder(key)
	if err != nil {
		glog.Errorf("inserting: %v failed (err: %v)", elem, err)
		return err
	}

	nodeToInsertAt := nodes[len(nodes)-1]
	// Add the copy of this node to origNodes before we insert.
	bpt.tracker.origNodes.AddIfNotPresent(nodeToInsertAt.NodeKey, nodeToInsertAt)
	tne := &treeNodeElem{NodeKey: common.Key{BPTkey: ""}, DataKey: key, Data: elem}
	nodeToInsertAt.insertElement(tne, bpt.context.maxDegree)
	bpt.tracker.updatedNodes.AddIfNotPresent(nodeToInsertAt.NodeKey, nodeToInsertAt)

	err = bpt.checkRebalance(nodes, indexes)
	err = bpt.update(err)
	if err != nil {
		glog.Errorf("inserting: %v:%v failed (err: %v)", key, elem, err)
	} else {
		glog.Infof("done inserting: %v:%v (success)", key, elem)
	}
	if glog.V(2) {
		bpt.Print()
		bpt.context.memMgr.Print()
	}
	return err
}

// Remove - Remove an element with the given key from the tree.
// Returns error if encoutered
func (bpt *BplusTree) Remove(key common.Key) error {
	keys := getKeysToLock(key)
	bpt.context.lockMgr.Lock(nil, keys)
	defer bpt.context.lockMgr.Unlock(nil, keys)

	if !bpt.initialized {
		glog.Errorf("tree not initialized")
		return common.ErrNotInitialized
	}
	if glog.V(2) {
		defer glog.Flush()
	}

	glog.Infof("removing: %v\n", key)
	nodes, indexes, err := bpt.removeFinder(key)
	if err != nil {
		glog.Errorf("removing: %v failed (err: %v)", key, err)
		return err
	}

	glog.V(2).Infof("accumulated following nodes to remove: %v\n", key)
	for _, n := range nodes {
		glog.Infof("<%v> ", n)
	}
	glog.V(2).Infof("and following indexes: %v", indexes)
	glog.V(2).Infof("\n----------------------------\n")

	nodeToRemoveFrom := nodes[len(nodes)-1]
	bpt.initTracker()
	bpt.tracker.origNodes.AddIfNotPresent(nodeToRemoveFrom.NodeKey, nodeToRemoveFrom)
	err = nodeToRemoveFrom.removeElement(key, bpt.context.maxDegree)
	if err != nil {
		glog.Errorf("removing: %v from node (%v) failed (err: %v)",
			key, nodeToRemoveFrom, err)
		return err
	}
	bpt.tracker.updatedNodes.AddIfNotPresent(nodeToRemoveFrom.NodeKey, nodeToRemoveFrom)

	err = bpt.checkMergeOrDistribute(nodes, indexes)
	err = bpt.update(err)
	if err != nil {
		glog.Infof("done removing: %v (err: %v)", key, err)
	} else {
		glog.Infof("done removing: %v. Success.\n", key)
	}

	if glog.V(2) {
		bpt.Print()
		bpt.context.memMgr.Print()
	}
	return err
}

// Search - Search for a given key (or more) in the tree. The SearchSpecifier
// argument specifies what needs to be searched. Please look at SearchSpecifier
// for more details on specifying keys to search for.
// Returns the slice of elements matchig the search criteria or error.
func (bpt *BplusTree) Search(ss SearchSpecifier) ([]Element, error) {
	result := make([]Element, 0) // We don't know the size.
	// Lets initialize the lock. We only need a read lock here.
	keys := getKeysToLock(ss.searchKey)
	bpt.context.lockMgr.Lock(keys, nil)
	defer bpt.context.lockMgr.Unlock(keys, nil)

	// Return error if not initialized.
	if !bpt.initialized {
		glog.Errorf("tree is not initialized\n")
		return nil, common.ErrNotInitialized
	}

	// XXX: Do some more sanity check for the input parameters.
	// What should be the max limit of # of elements? Using 50 for now.
	// Better would be to curtail the elems to 50? For now, return error.
	if ss.maxElems > 50 {
		glog.Warning("max elements too large, should be less than 50")
		return nil, common.ErrTooLarge
	}

	// Get the leaf node where the element should be.
	nodes, _, err := bpt.searchFinder(ss.searchKey)
	if err != nil {
		glog.Errorf("failed to find key: %v (err: %v)", ss.searchKey, err)
		return nil, err
	}

	if len(nodes) != 1 {
		panic("unexpected length")
	}

	node := nodes[0]

	// Do binary search in the leaf node.
	index, exactMatch := node.find(ss.searchKey)
	if index >= len(node.Children) || exactMatch != true {
		glog.V(2).Infof("index: %d, exactMatch: %v, node: %v", index, exactMatch, node)
		glog.Errorf("failed to find key: %v", ss.searchKey)
		return nil, common.ErrNotFound
	}

	matchingElem := node.Children[index]

	// If search is exact then we already found the element. Return that.
	// TODO: Return the key as well intead of just data. SS needs updating.
	result = append(result, matchingElem.Data)
	if ss.direction == Exact {
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

	// Setup the left index/markers to start accumulating elements.
	leftStart := 0
	leftEnd := index
	leftNode := node
	leftIndex := leftEnd - 1
	// If the exact match is at the beginning of a leaf, then the elements
	// to the left are in 'prev' leaf. Adjust for that.
	if index == 0 {
		leftNode, err = bpt.fetch(node.PrevKey)
		if leftNode != nil {
			leftEnd = len(leftNode.Children)
			leftIndex = len(leftNode.Children) - 1
		} else if err != nil {
			glog.Errorf("failed to fetch key %v (err: %v)", node.PrevKey, err)
			return nil, err
		}
	}

	// Setup right index/markers.
	rightStart := index + 1
	rightNode := node
	rightIndex := index + 1
	// If the exact match is at the end of a leaf, then the elements
	// to the right are in 'next' leaf. Adjust for that.
	if index == len(node.Children)-1 {
		rightNode, err = bpt.fetch(node.NextKey)
		if rightNode != nil {
			rightStart = 0
			rightIndex = 0
		} else if err != nil {
			glog.Errorf("failed to fetch key %v (err: %v)", node.NextKey, err)
			return nil, err
		}
	}

	// If we are not using the evaluator, we can accumulate in batches until
	// we have accumulated enough to meet the left and right width
	if ignoreEvaluator {
		// accumulating as many elements as needed to the left of exact match
		for numElemsLeft > 0 && leftNode != nil {
			if leftEnd == -1 {
				leftEnd = len(leftNode.Children)
			}
			if (leftEnd - leftStart) < numElemsLeft {
				result = append(bpt.accumulate(leftNode, leftStart, leftEnd),
					result...)
				numElemsLeft -= leftEnd - leftStart
				leftEnd = -1 // We need to reset it.
				leftNode, err = bpt.fetch(leftNode.PrevKey)
				if leftNode == nil && err != nil {
					glog.Errorf("failed to fetch key %v (err: %v)",
						leftNode.PrevKey, err)
					return result, err
				}
			} else {
				result = append(bpt.accumulate(leftNode, leftEnd-numElemsLeft,
					leftEnd), result...)
				break
			}
		}
		// accumulating as many elements as needed to the right of exact match
		for numElemsRight > 0 && rightNode != nil {
			rightEnd := len(rightNode.Children)
			if (rightEnd - rightStart) < numElemsRight {
				result = append(result, bpt.accumulate(rightNode, rightStart,
					rightEnd)...)
				numElemsRight -= rightEnd - rightStart
				rightStart = 0
				rightNode, err = bpt.fetch(rightNode.NextKey)
				if rightNode == nil && err != nil {
					glog.Errorf("failed to fetch key %v (err: %v)",
						rightNode.NextKey, err)
					return result, err
				}
			} else {
				result = append(result, bpt.accumulate(rightNode, rightStart,
					rightStart+numElemsRight)...)
				break
			}
		}
	} else {
		// Else case: If the evaluator is specified however, we need to
		// traverse linearly from the exact match to either accumulate as many
		// elements (as per the maxElems from left and/or right) or stop when
		// the evaluator stops evaluating to true even if we haven't
		// accumulated 'maxElems'.

		// Do it for the left side
		for numElemsLeft > 0 && leftNode != nil {
			elemToLeft := leftNode.Children[leftIndex]
			evaluatorLeftExhausted = !ss.evaluator(ss.searchKey,
				elemToLeft.DataKey)
			if evaluatorLeftExhausted {
				break
			}
			result = append([]Element{elemToLeft.Data}, result...)
			leftIndex--
			numElemsLeft--
			if leftIndex < 0 && !leftNode.PrevKey.IsNil() {
				leftNode, err = bpt.fetch(leftNode.PrevKey)
				if leftNode != nil {
					leftIndex = len(leftNode.Children) - 1
				} else {
					glog.Errorf("failed to fetch key %v (err: %v)",
						leftNode.PrevKey, err)
					return result, err
				}
			}
		}

		// Do it for the right side.
		for numElemsRight > 0 && rightNode != nil {
			elemToRight := rightNode.Children[rightIndex]
			evaluatorRightExhausted = !ss.evaluator(ss.searchKey,
				elemToRight.DataKey)
			if evaluatorRightExhausted {
				break
			}
			result = append(result, elemToRight.Data)
			rightIndex++
			numElemsRight--
			if rightIndex >= len(rightNode.Children) && !rightNode.NextKey.IsNil() {
				rightNode, err = bpt.fetch(rightNode.NextKey)
				if rightNode != nil {
					rightIndex = 0
				} else {
					glog.Errorf("failed to fetch key %v (err: %v)",
						leftNode.PrevKey, err)
					return result, err
				}
			}
		}
	}

	return result, nil
}

// Print - Prints the BplusTree on stdout.
func (bpt *BplusTree) Print() {
	enabled, freq := TestPointIsEnabled(testPointFailDBFetch)
	if enabled {
		TestPointDisable(testPointFailDBFetch)
	}
	bpt.writeTree(true)
	if enabled {
		TestPointEnable(testPointFailDBFetch, freq)
	}
}
