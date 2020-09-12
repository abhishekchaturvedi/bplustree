package bplustree

import (
	"fmt"
	"testing"
	"sync"
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

func (elem *TestingElem) GetKey() BplusTreeKey {
	return BplusTreeKey(elem.key)
}

func (elem *TestingElem) String() string {
	return fmt.Sprintf("key: %d, val: %d", elem.key, elem.val)
}

func (key *TestingKey) String() string {
	return fmt.Sprintf("key: %d", key)
}


var newtree *BplusTree
var numElems int = 12000
var maxDegree int = numElems / 10

func TestInit(t *testing.T) {
	ctx := BplusTreeCtx{lockMgr: nil, maxDegree: maxDegree}
	var err error
	newtree, err = NewBplusTree(ctx)
	if err != nil {
		t.Errorf("Failed to create new bplustree: %v", err)
		t.FailNow()
	}
}

func InsertRoutine(t *testing.T, elem *TestingElem, errors []error, index int, wg *sync.WaitGroup)  {
	defer wg.Done()
	t.Logf("Inserting: %v", elem)
	err := newtree.Insert(elem)
	errors[index] = err
	if err == nil {
		//newtree.Print()
	}
	t.Logf("++++++++++++++++++++++++++++++++")
}

func TestInsert(t *testing.T) {
	t.Log("Starting insert test..")
	elems := make([]TestingElem, numElems)
	errors := make([]error, numElems)
	var wg sync.WaitGroup

	for i := 0; i < numElems; i++ {
		elem := &elems[i]
		elem.key = TestingKey(i)
		elem.val = i + 10
		wg.Add(1)
		go InsertRoutine(t, elem, errors, i, &wg)
	}

	wg.Wait()
	for i := 0; i < numElems; i++ {
		if errors[i] != nil {
			t.Errorf("Failed to insert: %d: %v", i, errors[i])
			t.FailNow()
		}
	}
	t.Logf("\nFinal tree:\n")
	newtree.Print()
}
