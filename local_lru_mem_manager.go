// Use of this software is governed by an Apache 2.0
// licence which can be found in the license file

// This file implements a local single node memory manager which employs.
// LRU for eviction.

package bplustree

import (
	"bplustree/common"
	"container/list"
	"reflect"

	"github.com/golang/glog"
)

// LocalLRUMemMgr -- memory manager implementation. A default local memory
// manager data structure. Memory being used is just tracked in a map.
// ktype -- is the type of the key used for the map. See key.go for details.
// policy -- name of the policy used.
// size -- is the current # of items in the cache
// limit -- is the max # of items the cache is configured to have.
// lruLust -- list containing the elements for implementing LRU.
// memMap -- map for tracking the elements for fast lookup.
type LocalLRUMemMgr struct {
	ktype   common.KeyType
	policy  string
	size    int
	limit   int
	lruList *list.List
	memMap  map[common.Key]*list.Element
}

// listElem -- each list element contains the key/value
type listElem struct {
	key   common.Key
	value interface{}
}

// updateSize - Update the cache size due to addition or deletion of element.
// return true if size is greater than limit (i.e. cache is full).
//
// TODO: Ideally, we want to be able to track the size of each elem
// to enforce the size, but as of right now, using # of nodes for tracking
// the size and limit.
func (mgr *LocalLRUMemMgr) updateSize(add bool) {
	if add {
		mgr.size++
	} else {
		mgr.size--
	}
	glog.V(2).Infof("current size: %d, limit: %d",
		mgr.size, mgr.limit)
}

// NewLocalLRUMemMgr -- instantiates a new local LRU memmgr.
func NewLocalLRUMemMgr(limit int, kt common.KeyType, pfx string) *LocalLRUMemMgr {
	return &LocalLRUMemMgr{ktype: kt, policy: common.MemMgrPolicyLocalLRU, size: 0,
		limit: limit, lruList: new(list.List), memMap: make(map[common.Key]*list.Element, 0)}
}

// insertInt -- just insert, all sanity checks done.
func (mgr *LocalLRUMemMgr) insertInt(k common.Key, e interface{}) {
	le := &list.Element{Value: listElem{key: k, value: e}}
	ptr := mgr.lruList.PushFront(le)
	mgr.memMap[k] = ptr
	glog.V(2).Infof("inserting %v (%v)", ptr.Value, reflect.TypeOf(e))
}

// Insert - insert a key to be tracked. If some other key is getting
// evicted due to this insertion, the function will return errFul.
// This helps users know when eviction is taking place.
func (mgr *LocalLRUMemMgr) Insert(k common.Key, e interface{}) error {
	// Check if the key esists. If it does, then it needs to be promoted
	// to the head.
	tn, ok := mgr.memMap[k]
	var err error = nil

	if ok {
		// An update is still necessary since the value could change, so remove
		// the old element and push the new one to front.
		mgr.removeInt(k, tn)
		mgr.insertInt(k, e)
	} else { // element doesn't exist.

		// Since limit has to be enforced, update the size so we can check for
		// eviction.
		mgr.updateSize(true)
		if mgr.size > mgr.limit {
			key := mgr.lruList.Back().Value.(*list.Element).Value.(listElem).key
			glog.V(2).Infof("evicting: %v", key)
			mgr.removeInt(key, mgr.lruList.Back())
			mgr.updateSize(false)
		}
		// Insert the new element now.
		mgr.insertInt(k, e)
	}

	return err
}

// removeInt -- just remove the key. Sanity checks are already done.
func (mgr *LocalLRUMemMgr) removeInt(k common.Key, tn *list.Element) {
	glog.V(2).Infof("removing %v", tn.Value)
	delete(mgr.memMap, k)
	mgr.lruList.Remove(tn)
}

// Remove - Remove a key from tracking
func (mgr *LocalLRUMemMgr) Remove(k common.Key) error {
	tn, ok := mgr.memMap[k]
	if !ok {
		glog.Errorf("failed to find key: %v", k)
		return common.ErrNotFound
	}
	mgr.updateSize(false)
	mgr.removeInt(k, tn)
	return nil
}

// Lookup - Lookup a key
func (mgr *LocalLRUMemMgr) Lookup(k common.Key) (interface{}, error) {
	node, ok := mgr.memMap[k]
	if !ok {
		glog.Errorf("failed to find key: %v", k)
		return nil, common.ErrNotFound
	}
	// Update recency of the node.
	mgr.lruList.MoveToFront(node)
	val := node.Value.(*list.Element).Value.(listElem).value
	glog.V(2).Infof("found key: %v, %v (%v)", k, val, reflect.TypeOf(val))
	return val, nil
}

// Print the LRU
func (mgr *LocalLRUMemMgr) Print() {
	// Iterate through list and print its contents.
	glog.Infof("printing LRU list")
	for e := mgr.lruList.Front(); e != nil; e = e.Next() {
		glog.Infof("%v", e.Value)
	}
}

// Policy returns the string representation of memory manager policy.
func (mgr *LocalLRUMemMgr) Policy() string {
	return mgr.policy
}
