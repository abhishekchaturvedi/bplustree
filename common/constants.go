package common

// Constants used in bplus tree package.
const (
	MemMgrPolicyLocalLRU string = "local_lru"
	MemMgrPolicyLocalMap string = "local_hashmap"

	DBMgrPolicyLocalMap string = "local_hashmap"
	DBMgrPolicyADB      string = "arango_db_mgr"
)

// DBOpType -- types of DB operations
const (
	DBOpStore   DBOpType = 0
	DBOpDelete           = 1
	DBOpSetRoot          = 2
)
