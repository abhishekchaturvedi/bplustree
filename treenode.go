package bplustree

import "fmt"

// treeNode - The 'node' of the bplus tree. Each node is represented by the
// following data:
// 'children' is the list of children that this node has if not leaf node,
//            otherwise it contains the content/elements themselves.
// 'next' is the pointer to the next node (sibling to the right)
// 'prev' is the poitner to the prev node (sibling to the lef).
// 'isLeaf' whether this node is a leaf or not.
type treeNode struct {
	children Elements
	next     *treeNode
	prev     *treeNode
	isLeaf   bool
}

// insertElement - insert an element into the Elements of a given treeNode.
// returns appropriate error if insert fails.
func (node *treeNode) insertElement(elem Element, maxDegree int) error {
	children, err := node.children.insertSorted(elem, maxDegree)
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

// getLowestKey - Get the lowest key in the subtree rooted at treeNode.
// Retuns the lowest key from the subtree roooted at 'treeNode'
func (node *treeNode) getLowesetKey() Key {
	var tmpNode *treeNode = node
	for !tmpNode.isLeaf {
		tmpNode = tmpNode.children[0].(*treeNode)
	}
	return tmpNode.children[0].GetKey()
}

// GetKey - Each treeNode has to implement the 'Element' interface as well
// because at the leaf level, a treeNode is an element. However, when used, a
// treeNode may or may not be a leaf node. Thus the GetKey implementation gives
// the lowest key in the subtree rooted at treeNode.
// Retuns the lowest key from the subtree roooted at 'treeNode'
func (node *treeNode) GetKey() Key {
	return node.getLowesetKey()
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

	return fmt.Sprintf("{%p [children (len:%d): %v, prev: %v, next: %v, leaf: %v]}",
		node, len(node.children), node.children, prevKey, nextKey, node.isLeaf)
}
