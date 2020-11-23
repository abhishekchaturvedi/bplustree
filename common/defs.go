package common

// DBOpType -- type of DB operation that is to be done.
type DBOpType int

// DBOp -- A given DB operation.
// Op  - is the type of operation (store/delete)
// K   - is the key on which operation needs to be done.
// E   - optional value for the key (not used if operation is delete)
type DBOp struct {
	Op DBOpType
	K  Key
	E  interface{}
}
