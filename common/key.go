package common

import (
	"fmt"
	"hash/fnv"
)

// Key - Construct of a key to uniquely identify an entity.
// 'key' could be an arbitrary type.
// Compare function compares the given instance with the specified parameter.
// Returns -1, 0, 1 for less than, equal to and greater than respectively.
// ToString returns the string represenation of the key.
type Key struct {
	BPTkey string `json:"bptkey"`
}

func strHash(str string) uint32 {
	h := fnv.New32()
	h.Write([]byte(str))
	return h.Sum32()
}

func getKeyVal(k Key) int {
	var kval int
	fmt.Sscanf(k.BPTkey, "%d", &kval)
	return kval
}

/* // Compare -- compares the two strings
func (key1 *Key) Compare(key2 Key) int64 {
	k1val := getKeyVal(*key1)
	k2val := getKeyVal(key2)
	return int64(k1val - k2val)
} */

// Compare -- compares the two strings
func (key1 *Key) Compare(key2 Key) int64 {
	h1 := int64(strHash(key1.BPTkey))
	h2 := int64(strHash(key2.BPTkey))
	return h1 - h2
}

// ToString -- string representation
func (key1 *Key) ToString() string {
	return key1.BPTkey
}

// IsNil -- check for empty key.
func (key1 Key) IsNil() bool {
	if key1.BPTkey == "" {
		return true
	}
	return false
}

// KeyType - Type of key
type KeyType int

const (
	// UUIDType -- key is a uuid
	UUIDType KeyType = 0
	// RandI64Type -- random 64 bit int
	RandI64Type = 1
	// OrderedI64Type - monotonically increasing 64 bit int.
	OrderedI64Type = 2
	// RandStrType - random string.
	RandStrType = 3
	// OrderedStrType - string with an ordered int suffix.
	OrderedStrType = 4
)
