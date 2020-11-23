package bplustree

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"time"
)

// Seed used for random # generation.
var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

var orderedStrCount = 0

// Generating random string with following charset
const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func stringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func makeRandString(pfx string, length int) string {
	return fmt.Sprintf("%s_%s", pfx, stringWithCharset(length, charset))
}

func makeOrderedString(pfx string, idx int) string {
	return fmt.Sprintf("%s.key_%d", pfx, idx)
}

// Key - Construct of a key to uniquely identify an entity.
// 'key' could be an arbitrary type.
// Compare function compares the given instance with the specified parameter.
// Returns -1, 0, 1 for less than, equal to and greater than respectively.
type Key interface {
	Compare(key Key) int64
}

// KeyType - Type of key
type KeyType int

const (
	uuidType       KeyType = 0
	randI64Type            = 1
	orderedI64Type         = 2
	randStrType            = 3
	orderedStrType         = 4
)

var int64Count = 0

// Int64Key - Use int64 as a key.
type Int64Key int64

// Compare - Int64Key compare function
func (ik1 Int64Key) Compare(k2 Key) int64 {
	ik2 := k2.(Int64Key)
	return int64(ik1) - int64(ik2)
}

// RandStringKey -- A random string as a key.
type RandStringKey string

func randStrKeyHash(str string) uint32 {
	h := fnv.New32()
	h.Write([]byte(str))
	return h.Sum32()
}

// Compare - String key compare function
func (sk1 RandStringKey) Compare(k2 Key) int64 {
	sk2 := k2.(RandStringKey)
	hash1 := randStrKeyHash(string(sk1))
	hash2 := randStrKeyHash(string(sk2))
	return int64(hash1 - hash2)
}

// OrderedStringKey -- A string key with format "key_<int>"
type OrderedStringKey string

func orderedStrKeyHash(str string) int64 {
	var index int64

	fmt.Sscanf(string(str), "key_%d", &index)
	return index
}

// Compare - Ordered String key compare function
func (sk1 OrderedStringKey) Compare(k2 Key) int64 {
	sk2 := k2.(OrderedStringKey)
	hash1 := orderedStrKeyHash(string(sk1))
	hash2 := orderedStrKeyHash(string(sk2))
	return int64(hash1 - hash2)
}

// Generate -- Generate a key of given type.
func Generate(kt KeyType, pfx string) Key {
	switch kt {
	case randI64Type:
		return Int64Key(seededRand.Int63())
	case randStrType:
		return RandStringKey(makeRandString(pfx, 16))
	case orderedStrType:
		orderedStrCount++
		return OrderedStringKey(makeOrderedString(pfx, orderedStrCount))
	case orderedI64Type:
		int64Count++
		return Int64Key(int64Count)
	}
	return nil
}
