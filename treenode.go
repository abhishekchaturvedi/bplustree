// Use of this software is governed by an Apache 2.0
// licence which can be found in the license file

package bplustree

import (
	"bplustree/common"

	"fmt"
	"sort"

	"github.com/golang/glog"
)

// treeNodeElem -- This structure represent each element in the slice/array
// within a treeNode. If the treeNode is a leaf node, then it will contain
// optional data, and the nodeKey will be nil, but otherwise nodeKey is the
// key for the child node and dataKey is the key for user data rooted at that
// child node.
type treeNodeElem struct {
	NodeKey common.Key `json:"nodekey"`
	DataKey common.Key `json:"datakey"`
	Data    Element    `json:"data"`
}

// treeNode - The 'node' of the bplus tree. Each node is represented by the
// following data:
// 'children' is the list of children that this node has if not leaf node,
//            otherwise it contains the content/elements themselves.
// 'next' is the pointer to the next node (sibling to the right)
// 'prev' is the poitner to the prev node (sibling to the lef).
// 'isLeaf' whether this node is a leaf or not.
type treeNode struct {
	IsLeaf   bool           `json:"isleaf"`
	PrevKey  common.Key     `json:"prevkey"`
	NextKey  common.Key     `json:"nextkey"`
	NodeKey  common.Key     `json:"nodekey"`
	DataKey  common.Key     `json:"datakey"`
	Children []treeNodeElem `json:"children"`
}

// find - Find the specified key in the list of chidren of this node
// Returns the index of element with either the matching key or the key which
// is before the key (in sort oder) if the matching key isn't found.
func (node *treeNode) find(key common.Key) (int, bool) {
	exactMatch := false
	index := sort.Search(len(node.Children), func(i int) bool {
		ret := node.Children[i].DataKey.Compare(key)
		if ret == 0 {
			exactMatch = true
		}
		return ret >= 0
	})
	return index, exactMatch
}

// updateDataKey - Update the node's data Key value. When a new element is inserted, let
// us update the dataKey of this node to be the lower of the two (current
// dataKey of the node or the element's key)
// returns whether the node was updated or not.
func (node *treeNode) updateDataKey(bpt *BplusTree, cIdx int, elem *treeNodeElem) (bool, error) {

	if cIdx == -1 { // Updating based on self.
		if elem == nil {
			if len(node.Children) > 0 {
				node.DataKey = node.Children[0].DataKey
				return true, nil
			}
		} else if node.DataKey.IsNil() || node.DataKey.Compare(elem.DataKey) >= 0 {
			node.DataKey = elem.DataKey
			return true, nil
		}
	} else if bpt != nil && !node.IsLeaf && cIdx < len(node.Children) {
		child, err := bpt.fetch(node.Children[cIdx].NodeKey)
		if err != nil {
			glog.Errorf("Failed to fetch key %v (err: %v)",
				node.Children[cIdx].NodeKey, err)
			return false, err
		}
		if node.DataKey.IsNil() || node.DataKey.Compare(child.DataKey) >= 0 {
			node.DataKey = child.DataKey
			node.Children[cIdx].DataKey = child.DataKey
			return true, nil
		}
	}
	return false, nil
}

// insertElement - insert an element into the Elements of a given treeNode.
func (node *treeNode) insertElement(elem *treeNodeElem, size int) {

	index, _ := node.find(elem.DataKey)

	// Insert at the end case.
	if index >= len(node.Children) {
		node.Children = append(node.Children, *elem)
		if index == 0 {
			node.updateDataKey(nil, -1, elem)
		}
		return
	}

	// If inserting in the middle.
	newChildren := make([]treeNodeElem, len(node.Children)+1, size+1)
	copy(newChildren, node.Children[:index])
	newChildren[index] = *elem
	copy(newChildren[index+1:], node.Children[index:])
	node.Children = newChildren
	if index == 0 {
		node.updateDataKey(nil, -1, elem)
	}
}

// removeElement - remove an element from the Elements of given treeNode
// Returns appropriate error if remove fails.
func (node *treeNode) removeElement(key common.Key, maxDegree int) error {
	index, exactMatch := node.find(key)
	if !exactMatch {
		return common.ErrNotFound
	}

	if index >= len(node.Children) {
		glog.Errorf("Found element at index: %d in array of len: %d",
			index, len(node.Children))
		return common.ErrNotFound
	}
	node.Children = append(node.Children[:index], node.Children[index+1:]...)
	node.updateDataKey(nil, -1, nil)
	return nil
}

// deepcopy - copy all data from src treeNode to a new treeNode and return that
func (node *treeNode) deepCopy() *treeNode {
	dst := &treeNode{node.IsLeaf, node.PrevKey, node.NextKey, node.NodeKey,
		node.DataKey, append([]treeNodeElem{}, node.Children...)}
	if node.IsLeaf {
		for i := range node.Children {
			dst.Children[i].Data = node.Children[i].Data
		}
	}
	return dst
}

// String - stringigy treeNode
// Returns the string representation of the treeNode.
func (node *treeNode) String() string {
	childStr := ""
	for _, child := range node.Children {
		childStr += fmt.Sprintf("[%v, %v]", child.NodeKey, child.DataKey)
	}
	return fmt.Sprintf("{%p: L: %v, DK: %v, NK: %v, PK: %v, XK: %v [children (len:%d): %v]}",
		node, node.IsLeaf, node.DataKey, node.NodeKey, node.PrevKey,
		node.NextKey, len(node.Children), childStr)
}
