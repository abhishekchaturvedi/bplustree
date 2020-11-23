# bplustree
Go implementation of generic BplusTree

Details on some features:

This package implements a persistent B+Tree with a generic caching layer, 
persistent layer, and locker interface. 

A sample memory manager and persistent layer (db) manager and lock manager is
included. Sample memory manager is a LRU based implementation (local_lru_mem_manager.go) 
which tracks configurable # of tree nodes. One can use any distributed cache as
needed for memory manager. Sample DB manager is a in-memory key-value map 
(local_hashmap_db_manager) which can be replaced with any other local or distributed DB layer.
Sample lock manager implements local locking using mutex. One can use any distributed
locking when using persistence layer etc.


Build and Run tests.

- change directory to the bplustree directory.

build: 
To build just execute following command.

go build

test: 
To test run the following command.
go test [-v[=1 -vmodule=*=2]]
