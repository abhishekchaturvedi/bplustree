// Use of this software is governed by an Apache 2.0
// licence which can be found in the license file

// This file implements an hash map based single node DB driver.

package bplustree

import (
	"bplustree/common"

	"github.com/golang/glog"
)

// LocalHashMapDBMgr -- memory based DB implementation. DB is just a hash map
type LocalHashMapDBMgr struct {
	memMap map[common.Key]interface{}
	ktype  common.KeyType
	prefix string
	policy string
	base   Base
}

// NewLocalHashMapDBMgr -- instantiates a new local LRU memmgr.
func NewLocalHashMapDBMgr(pfx string) *LocalHashMapDBMgr {
	return &LocalHashMapDBMgr{memMap: make(map[common.Key]interface{}),
		policy: common.DBMgrPolicyLocalMap, prefix: pfx, base: Base{common.Key{BPTkey: ""}, 0}}

}

// GetRoot - Get root base for the tree.
func (mgr *LocalHashMapDBMgr) GetRoot() (*Base, error) {
	if mgr.base.RootKey.IsNil() || mgr.base.Degree == 0 {
		return nil, nil
	}
	return &mgr.base, nil
}

// SetRoot - Set root base for the tree.
func (mgr *LocalHashMapDBMgr) SetRoot(base *Base) error {
	mgr.base.Degree = base.Degree
	mgr.base.RootKey = base.RootKey
	return nil
}

// Store - store a key in the DB
func (mgr *LocalHashMapDBMgr) Store(k common.Key, e interface{}) error {
	tn := e.(*treeNode)
	mgr.memMap[k] = tn.deepCopy()
	glog.V(1).Infof("storing %v in db (val: %v)", k, mgr.memMap[k])
	return nil
}

// Delete - deletes a key from the db
func (mgr *LocalHashMapDBMgr) Delete(k common.Key) error {
	glog.V(1).Infof("deleting %v from db", k)
	delete(mgr.memMap, k)
	return nil
}

// AtomicUpdate - Updates the DB atomically with the provided ops.
func (mgr *LocalHashMapDBMgr) AtomicUpdate(ops []common.DBOp) error {
	for i := 0; i < len(ops); i++ {
		switch {
		case ops[i].Op == common.DBOpStore:
			mgr.Store(ops[i].K, ops[i].E)
		case ops[i].Op == common.DBOpDelete:
			mgr.Delete(ops[i].K)
		case ops[i].Op == common.DBOpSetRoot:
			mgr.SetRoot(ops[i].E.(*Base))
		}
	}
	return nil
}

// Load - Load a key from the DB
func (mgr *LocalHashMapDBMgr) Load(k common.Key) (interface{}, error) {
	node, ok := mgr.memMap[k]
	glog.V(2).Infof("loading %v from db", k)
	if !ok {
		return nil, common.ErrNotFound
	}
	glog.V(2).Infof("successfully loaded %v (%v) from db", k, node)
	return node.(*treeNode).deepCopy(), nil
}

// LogAllKeys -- Prints the content of memory manager
func (mgr *LocalHashMapDBMgr) LogAllKeys() {
	for k, e := range mgr.memMap {
		glog.Infof("%v: %v", k, e)
	}
}

// Policy -- Get the policy name for this manager.
func (mgr *LocalHashMapDBMgr) Policy() string {
	return mgr.policy
}
