// Use of this software is governed by an Apache 2.0
// licence which can be found in the license file

// This file implements a dbmanager interface to store and load keys from a
// backend DB, which typically can be a key value store.

package bplustree

import (
	"bplustree/common"
)

// DBMgr - The DB Manager interface mentiond the API that a persistence layer needs
// to support to store the bplustree on disk. We are going to store the bplustree as
// as a set of key-value pairs in the underlying store.
type DBMgr interface {
	// Get the root base from the DB. DB layer can choose to store the root key
	// in whichever fashion that they want. Instantiation of a BplusTree always
	// attempt to load the root key from the DB manager using this interface.
	// It should return:
	// (validBase, nil) if a valid root key/base is there.
	// (nil, error) if key/base wasn't found due to some DB error.
	// (nil, nil) if there is no key/base because it was never created. Only in this
	// case a new bplustree instance will be created as no key exists and there
	// was no error.
	GetRoot() (*Base, error)
	// Whenever the root key is modified due to updates to the b+tree, if that
	// results in root key getting updated, the bplustree implementation will
	// use this interface to update the root with the db layer.
	SetRoot(*Base) error
	// Load gets the key from the undelying db and returns the value assocaited with the key.
	Load(common.Key) (interface{}, error)
	// Store is used to store a key and corresponding value in to the DB.
	Store(k common.Key, e interface{}) error
	// Delete is used to delete a key from the underlying persistent store.
	// Once delete is called the key can no longer with recovered
	Delete(k common.Key) error

	// Atomically Update is used to update (store new/delete or update) a list
	// of key from the underlying persistent store. This operaton should be
	// atomic.
	AtomicUpdate(ops []common.DBOp) error
	// Log all keys stored in the DB
	LogAllKeys()
}
