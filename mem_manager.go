// Use of this software is governed by an Apache 2.0
// licence which can be found in the license file

// This file implements a memory manager interface to store the keys and values
// of a bplustree. In addition to storing the values, the memory manager may
// employ any policy for eviction etc. This file just provides the interface.
// The underlying implementation to be used will be looked up for as a plugin
// and used. For example, local_lru_mem_manager.go implements a LRU based
// eviction policy for mem manager for single node.
// For actual use in a distributed cluster, use a distributed implementation backed
// by a KV store like redis or memcache.

package bplustree

import "bplustree/common"

// MemMgr - The Memory Manager interface needs to be implemented by the user if
// they want this BplusTree library to use user provided interface for
// allocation. The memory manager could have it's own internal policy on how
// much to cache etc.
// Following methods have to be implemented by the data structure which
// implements this interface.
// Insert -- Inserting a node to be tracked by memMgr.
// Remove -- Remove a node from tracking by memMgr
// Lookup -- Lookup a node key in memMgr
// Print  -- Prints memory manager info/keys.
// Policy -- Returns memory manager's policy (lru/xyz, etc.)
type MemMgr interface {
	// Alloc inserts a new key into the memoy manager. An old key may get evicted
	// if the memory size limit is reached
	Insert(k common.Key, e interface{}) error
	Remove(k common.Key) error
	Lookup(k common.Key) (interface{}, error)
	Print()
	Policy() string
}
