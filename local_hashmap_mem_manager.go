// Use of this software is governed by an Apache 2.0
// licence which can be found in the license file

// This file implements an hash map based single node memory manager.
// without any memory limits.

package bplustree

import (
	"bplustree/common"

	"github.com/golang/glog"
)

// LocalHashMapMemMgr -- memory manager implementation. A default local memory
// manager data structure. Memory being used is just tracked in a map.
type LocalHashMapMemMgr struct {
	memMap map[common.Key]interface{}
	ktype  common.KeyType
	prefix string
	policy string
}

// NewLocalHashMapMemMgr -- instantiates a new local LRU memmgr.
func NewLocalHashMapMemMgr(kt common.KeyType, pfx string) *LocalHashMapMemMgr {
	return &LocalHashMapMemMgr{ktype: kt, policy: common.MemMgrPolicyLocalMap,
		prefix: pfx, memMap: make(map[common.Key]interface{})}
}

// Insert - insert a key to be tracked.
func (mgr *LocalHashMapMemMgr) Insert(k common.Key, e interface{}) error {
	mgr.memMap[k] = e
	return nil
}

// Remove - Remove a key from tracking
func (mgr *LocalHashMapMemMgr) Remove(k common.Key) error {
	delete(mgr.memMap, k)
	return nil
}

// Lookup - Lookup a key
func (mgr *LocalHashMapMemMgr) Lookup(k common.Key) (interface{}, error) {
	node, ok := mgr.memMap[k]
	if !ok {
		return nil, common.ErrNotFound
	}
	return node, nil
}

// Print -- prints the map
func (mgr *LocalHashMapMemMgr) Print() {
	glog.Infof("%v", mgr.memMap)
}

// Policy returns the string representation of memory manager policy.
func (mgr *LocalHashMapMemMgr) Policy() string {
	return mgr.policy
}
