package bplustree

import (
	// "fmt"
	"testing"
	// "time"
)

type TestingKey int
func (k1 TestingKey) Compare (key BplusTreeKey) int {
	k2 := key.(TestingKey)
	return int(k1) - int(k2)
}

type TestingElem struct {
	key TestingKey
	val int
}

func (elem *TestingElem) GetKey () BplusTreeKey {
	return BplusTreeKey(elem.key)
}

func TestInit(t *testing.T) {
	ctx := BplusTreeCtx{lockMgr: nil, maxDegree: 10}
	_, err := NewBplusTree(ctx)
	if err != nil {
		t.Errorf("Failed to create new bplustree: %v", err)
		t.FailNow()
	}
}
