package bplustree

import (
	"fmt"
	"sort"
)

// Elements - An array of Element
type Elements []Element

// find - Find the specified key in the elements slice.
// Returns the index of element with either the matching key or the key which
// is before the key (in sort oder) if the matching key isn't found.
func (elems Elements) find(key Key) (int, bool) {
	exactMatch := false
	index := sort.Search(len(elems), func(i int) bool {
		ret := elems[i].GetKey().Compare(key)
		if ret == 0 {
			exactMatch = true
		}
		return ret >= 0
	})
	return index, exactMatch
}

// insert - Inserts an element to the Elements array in order.
// Returns the updated Elements slice.
func (elems Elements) insertSorted(elem Element, size int) (Elements, error) {
	index, _ := elems.find(elem.GetKey())
	// Insert at the end case.
	if index >= len(elems) {
		elems = append(elems, elem)
		return elems, nil
	}

	// If inserting in the middle.
	// XXX: Will need to use memMgr here.
	newElems := make(Elements, len(elems)+1, size+1)
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
		fmt.Printf("Found element at index: %d in array of len: %d",
			index, len(elems))
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
