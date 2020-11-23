package common

import (
	"fmt"
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

// MakeRandString -- Makes a random string with given prefix and random
// suffix of specified length.
func MakeRandString(pfx string, length int) string {
	return fmt.Sprintf("%s_%s", pfx, stringWithCharset(length, charset))
}

func makeOrderedString(pfx string, idx int) string {
	return fmt.Sprintf("%s.key_%d", pfx, idx)
}

var int64Count = 0

// Int64Key - Use int64 as a key.
type Int64Key int64

// Generate -- Generate a key of given type.
func Generate(kt KeyType, pfx string) Key {
	switch kt {
	case RandI64Type:
		return Key{fmt.Sprintf("%d", Int64Key(seededRand.Int63()))}
	case RandStrType:
		return Key{MakeRandString(pfx, 16)}
	case OrderedStrType:
		orderedStrCount++
		return Key{makeOrderedString(pfx, orderedStrCount)}
	case OrderedI64Type:
		int64Count++
		return Key{fmt.Sprintf("%d", Int64Key(int64Count))}
	}
	return Key{""}
}
