// Use of this software is governed by an Apache 2.0
// licence which can be found in the license file

// This file implements a locker interface and a super simple in-memory
// implementation of the locker interface using mutex.
// For actual use in a distributed cluster, use a distributed lock
// implementation instead.

package bplustree

import (
	"bplustree/common"
	"sync"
)

// Locker - This is the locker interface. The clients of the library
// should fill in theimplementation for the library to use. The context to the
// lock/unlock calls should be the set of key we are operating on. Lock and
// Unlock takes the set of keys for which lock needs to be acquired as well as
// whether it is a readonly lock vs. a read-write lock.
// Each of the lock/unlock calls are going to be used for Insert/Delete/Lookup
// operations on the Tree.
type Locker interface {
	Init()
	Lock(readSet []common.Key, writeSet []common.Key)
	Unlock(readSet []common.Key, writeSet []common.Key)
}

// defaultLock - A default locker implementation using sync.RWMutex.
type defaultLock struct {
	mux *sync.RWMutex
}

func (lck *defaultLock) Init() {
	lck.mux = &sync.RWMutex{}
}
func (lck *defaultLock) Lock(readSet []common.Key, writeSet []common.Key) {
	if len(readSet) > 0 {
		lck.mux.RLock()
	} else {
		lck.mux.Lock()
	}
}
func (lck *defaultLock) Unlock(readSet []common.Key, writeSet []common.Key) {
	if len(readSet) > 0 {
		lck.mux.RUnlock()
	} else {
		lck.mux.Unlock()
	}
}
