package bplustree

import (
	"fmt"
	"log"
	"sync"
	"testing"
)

type TestingKey int

func (key TestingKey) Compare(key2 BplusTreeKey) int {
	k2 := key2.(TestingKey)
	return int(key) - int(k2)
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
var numElems int = 120
var maxDegree int = numElems / 10

func keyEvaluator(k1 BplusTreeKey, k2 BplusTreeKey) bool {
	result := k1.Compare(k2)
	if result < 10 && result > -10 {
		return true
	}

	return false
}
func TestInit(t *testing.T) {
	ctx := BplusTreeCtx{lockMgr: nil, maxDegree: maxDegree}
	var err error
	// Enable line numbers in logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	newtree, err = NewBplusTree(ctx)
	if err != nil {
		t.Errorf("Failed to create new bplustree: %v", err)
		t.FailNow()
	}
}

func InsertRoutine(t *testing.T, elem *TestingElem, errors []error, index int, wg *sync.WaitGroup) {
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

func TestSearch0(t *testing.T) {
	t.Log("Starting search tests..")

	// case 0. Look for a key which doesn't exist.
	var testingKey TestingKey = 10000
	ss := BplusTreeSearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err == nil {
		t.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result != nil {
		t.Errorf("Expected to not get any result but got: %v", result)
		t.FailNow()
	}

}

func TestSearch1(t *testing.T) {
	// case 1. Exact search.
	var testingKey TestingKey = 100
	ss := BplusTreeSearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err != nil {
		t.Errorf("Failed find the key: %v, err: %v", ss.searchKey, err)
		t.FailNow()
	}
	if result == nil {
		t.Errorf("Got nil result")
		t.FailNow()
	}

	// case 1.a Exact search. Provide an evaluator and maxElems which
	// should be ignored.
	var testingKey1 TestingKey = 100
	ss = BplusTreeSearchSpecifier{searchKey: testingKey1, direction: Exact,
		maxElems: 50, evaluator: keyEvaluator}
	result, err = newtree.Search(ss)
	if len(result) > 1 {
		t.Errorf("Expected to find exact match, but got more (%d)", len(result))
		t.FailNow()
	}
	if err != nil {
		t.Errorf("Expected to find the key: %v (err: %v)", ss.searchKey, err)
		t.FailNow()
	}
	if result == nil {
		t.Errorf("Got nil result")
		t.FailNow()
	}
}

func TestSearch2(t *testing.T) {
	// case 2. maxElems greater than limit.
	var testingKey TestingKey = 100
	ss := BplusTreeSearchSpecifier{searchKey: testingKey, direction: Exact,
		maxElems: 100, evaluator: keyEvaluator}

	result, err := newtree.Search(ss)
	if err == nil {
		t.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result != nil {
		t.Errorf("Expected to not get any result but got: %v", result)
		t.FailNow()
	}
}

func TestSearch3(t *testing.T) {
	// case 3. maxElems = 10, in Right direction. nil evaluator
	var testingKey TestingKey = 100
	ss := BplusTreeSearchSpecifier{searchKey: testingKey, direction: Right,
		maxElems: 10}

	result, err := newtree.Search(ss)
	if err != nil {
		t.Errorf("Expected to find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result == nil {
		t.Errorf("Expected to get result but nil: %v", result)
		t.FailNow()
	}

	if len(result) < ss.maxElems || len(result) > ss.maxElems+1 {
		t.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems, len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	expectedVal := int(testingKey + 10)
	for i := 0; i < len(result); i++ {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElem).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal, actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func TestSearch4(t *testing.T) {
	// case 4. maxElems = 10, in Left direction.
	var testingKey TestingKey = 100
	ss := BplusTreeSearchSpecifier{searchKey: testingKey, direction: Left,
		maxElems: 10}

	result, err := newtree.Search(ss)
	if err != nil {
		t.Errorf("Expected to find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result == nil {
		t.Errorf("Expected to get result but nil: %v", result)
		t.FailNow()
	}

	if len(result) < ss.maxElems || len(result) > ss.maxElems+1 {
		t.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems, len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	expectedVal := int(testingKey + 10)
	for i := len(result) - 1; i >= 0; i-- {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElem).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal, actualVal)
			t.FailNow()
		}
		// and decrement by 1.
		expectedVal--
	}
}

func TestSearch5(t *testing.T) {
	// case 5. maxElems = 10, in both directions.
	var testingKey TestingKey = 100
	ss := BplusTreeSearchSpecifier{searchKey: testingKey, direction: Both,
		maxElems: 20}

	result, err := newtree.Search(ss)
	if err != nil {
		t.Errorf("Expected to find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result == nil {
		t.Errorf("Expected to get result but nil: %v", result)
		t.FailNow()
	}

	if len(result) < ss.maxElems || len(result) > ss.maxElems+1 {
		t.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems, len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	startKey := int(testingKey) - 10
	expectedVal := int(startKey + 10)
	for i := 0; i < len(result); i++ {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElem).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal, actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func TestSearch6(t *testing.T) {

	// case 6. use of evaluator with maxElems = 10, such that
	// evaluator yields more than 10 elements. We should still
	// get 10 elements.

	var testingKey TestingKey = 100
	ss := BplusTreeSearchSpecifier{searchKey: testingKey, direction: Both,
		maxElems: 10, evaluator: func(k1 BplusTreeKey, k2 BplusTreeKey) bool {
			result := k1.Compare(k2)
			if result < 10 && result > -10 {
				return true
			}

			return false
		},
	}

	result, err := newtree.Search(ss)
	if err != nil {
		t.Errorf("Expected to find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result == nil {
		t.Errorf("Expected to get result but nil: %v", result)
		t.FailNow()
	}

	if len(result) < ss.maxElems || len(result) > ss.maxElems+1 {
		t.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems, len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	startKey := int(testingKey) - 5
	expectedVal := int(startKey + 10)
	for i := 0; i < len(result); i++ {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElem).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal, actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func TestSearch7(t *testing.T) {

	// case 7. use of evaluator with maxElems = 10, such that
	// evaluator yields less than 10 elements. We should only
	// get less than 10 elements.

	var testingKey TestingKey = 100
	ss := BplusTreeSearchSpecifier{searchKey: testingKey, direction: Both,
		maxElems: 10, evaluator: func(k1 BplusTreeKey, k2 BplusTreeKey) bool {
			result := k1.Compare(k2)
			if result < 3 && result > -3 {
				return true
			}

			return false
		},
	}

	result, err := newtree.Search(ss)
	if err != nil {
		t.Errorf("Expected to find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result == nil {
		t.Errorf("Expected to get result but nil: %v", result)
		t.FailNow()
	}

	if len(result) != 5 {
		t.Errorf("Expected to find 5 results, got: %d", len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	startKey := int(testingKey) - 2
	expectedVal := int(startKey + 10)
	for i := 0; i < len(result); i++ {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElem).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal, actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}
