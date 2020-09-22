package bplustree

import (
	"fmt"
	"log"
	"sync"
	"testing"
)

type TestingKey int64

func (key TestingKey) Compare(key2 Key) int {
	k2 := key2.(TestingKey)
	return int(int64(key) - int64(k2))
}

type TestingElem struct {
	key TestingKey
	val int
}

func (elem *TestingElem) GetKey() Key {
	return Key(elem.key)
}

func (elem *TestingElem) String() string {
	return fmt.Sprintf("key: %d, val: %d", elem.key, elem.val)
}

func (key *TestingKey) String() string {
	return fmt.Sprintf("key: %d", key)
}

type TestingKeyStr string

// A typical string hash function would be a fnv or some
// such, but for our testing, we want to have more deterministic
// behavior so that validation can be done easily (otherwise,
// we won't know what hash or next key to expect when validating.
// Also, our TestingKeyStr is being created with 'key_<int>' format
// so, here in our hash function, we are just going to use trailing
// _int part for computation of hash to make things predictable and
// verifiable.)
/* func strHash(str string) uint32 {
	h := fnv.New32()
	h.Write([]byte(str))
	return h.Sum32()
}
*/
func strHash(str TestingKeyStr) int {
	var index int

	fmt.Sscanf(string(str), "key_%d", &index)
	return index
}

func (key TestingKeyStr) Compare(key2 Key) int {
	k2Str := key2.(TestingKeyStr)
	k1Str := key

	k1hash := strHash(k1Str)
	k2hash := strHash(k2Str)
	return int(k1hash - k2hash)
}

type TestingElemStr struct {
	key TestingKeyStr
	val int
}

func (elem *TestingElemStr) GetKey() Key {
	return Key(elem.key)
}

func (elem *TestingElemStr) String() string {
	return fmt.Sprintf("key: %s, val: %d", elem.key, elem.val)
}

func (key *TestingKeyStr) String() string {
	str := string(*key)

	return fmt.Sprintf("key: %s", str)
}

var newtree *BplusTree
var numElems int = 100000
var maxDegree int = numElems / 100

func keyEvaluator(k1 Key, k2 Key) bool {
	result := k1.Compare(k2)
	if result < 10 && result > -10 {
		return true
	}

	return false
}
func TestInit(t *testing.T) {
	ctx := Context{lockMgr: nil, maxDegree: maxDegree}
	var err error
	// Enable line numbers in logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	newtree, err = NewBplusTree(ctx)
	if err != nil {
		t.Errorf("Failed to create new bplustree: %v", err)
		t.FailNow()
	}
}

func InsertRoutine(t *testing.T,
	elem *TestingElem,
	errors []error,
	index int,
	wg *sync.WaitGroup) {
	defer wg.Done()
	t.Logf("Inserting: %v", elem)
	err := newtree.Insert(elem)
	errors[index] = err

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
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
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
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
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
	ss = SearchSpecifier{searchKey: testingKey1, direction: Exact,
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
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact,
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
	ss := SearchSpecifier{searchKey: testingKey, direction: Right,
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
	ss := SearchSpecifier{searchKey: testingKey, direction: Left,
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
	ss := SearchSpecifier{searchKey: testingKey, direction: Both,
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
	ss := SearchSpecifier{searchKey: testingKey, direction: Both,
		maxElems: 10, evaluator: func(k1 Key, k2 Key) bool {
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
	ss := SearchSpecifier{searchKey: testingKey, direction: Both,
		maxElems: 10, evaluator: func(k1 Key, k2 Key) bool {
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

func TestRemove1(t *testing.T) {
	fmt.Println("Starting remove in order tests")
	for i := 0; i < numElems; i++ {
		key := TestingKey(i)
		fmt.Printf("removing key: %v", key)
		err := newtree.Remove(key)
		if err != nil {
			t.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
	}
}

func TestRemove2(t *testing.T) {
	TestInsert(t)
	fmt.Println("Starting remove in reverse order tests")
	for i := numElems - 1; i >= 0; i-- {
		key := TestingKey(i)
		fmt.Printf("removing key: %v", key)
		err := newtree.Remove(key)
		if err != nil {
			t.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
	}
}

func TestRemove3(t *testing.T) {
	TestInsert(t)
	fmt.Println("Starting remove in weird order test")

	removed := make(map[int]bool)
	for i := 0; i < numElems/3; i++ {
		k1 := i
		key := TestingKey(k1)
		fmt.Printf("removing key: %v", key)
		err := newtree.Remove(key)
		if err != nil {
			t.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
		removed[k1] = true

		k2 := numElems/3 + i
		key = TestingKey(k2)
		fmt.Printf("removing key: %v", key)
		err = newtree.Remove(key)
		if err != nil {
			t.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
		removed[k2] = true

		k3 := numElems*2/3 + i
		key = TestingKey(k3)
		fmt.Printf("removing key: %v", key)
		err = newtree.Remove(key)
		if err != nil {
			t.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
		removed[k3] = true
	}

	for i := 0; i < numElems; i++ {
		if !removed[i] {
			newtree.Remove(TestingKey(i))
		}
	}
}

func RemoveRoutine(t *testing.T, k int, errors []error, index int, wg *sync.WaitGroup) {
	defer wg.Done()
	t.Logf("Removing: %v", k)
	err := newtree.Remove(TestingKey(k))
	errors[index] = err
	t.Logf("++++++++++++++++++++++++++++++++")
}

func TestRemove4(t *testing.T) {
	TestInsert(t)
	fmt.Println("Starting remove in parallel test")
	errors := make([]error, numElems)
	var wg sync.WaitGroup

	for i := 0; i < numElems; i++ {
		wg.Add(1)
		go RemoveRoutine(t, i, errors, i, &wg)
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

func TestInitStr(t *testing.T) {
	ctx := Context{lockMgr: nil, maxDegree: maxDegree}
	var err error
	// Enable line numbers in logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	newtree, err = NewBplusTree(ctx)
	if err != nil {
		t.Errorf("Failed to create new bplustree: %v", err)
		t.FailNow()
	}
}

func InsertRoutineStr(t *testing.T,
	elem *TestingElemStr,
	errors []error,
	index int,
	wg *sync.WaitGroup) {
	defer wg.Done()
	t.Logf("Inserting: %v", elem)
	err := newtree.Insert(elem)
	errors[index] = err

	t.Logf("++++++++++++++++++++++++++++++++")
}

func makeTestingKeyStr(k int) TestingKeyStr {
	return TestingKeyStr(fmt.Sprintf("key_%d", k))
}

func TestInsertStr(t *testing.T) {
	t.Log("Starting insert test..")
	elems := make([]TestingElemStr, numElems)
	errors := make([]error, numElems)
	var wg sync.WaitGroup

	for i := 0; i < numElems; i++ {
		elem := &elems[i]
		elem.key = makeTestingKeyStr(i)
		elem.val = i + 10
		wg.Add(1)
		go InsertRoutineStr(t, elem, errors, i, &wg)
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

func TestSearchStr0(t *testing.T) {
	t.Log("Starting search tests..")

	// case 0. Look for a key which doesn't exist.
	testingKey := makeTestingKeyStr(10000)
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
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

func keyEvaluatorStr(k1 Key, k2 Key) bool {
	result := k1.Compare(k2)
	if result < 10 && result > -10 {
		return true
	}

	return false
}
func TestSearchStr1(t *testing.T) {
	// case 1. Exact search.
	testingKey := makeTestingKeyStr(100)
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
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
	testingKey1 := makeTestingKeyStr(100)
	ss = SearchSpecifier{searchKey: testingKey1, direction: Exact,
		maxElems: 50, evaluator: keyEvaluatorStr}
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

func TestSearchStr2(t *testing.T) {
	// case 2. maxElems greater than limit.
	testingKey := makeTestingKeyStr(100)
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact,
		maxElems: 100, evaluator: keyEvaluatorStr}

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

func TestSearchStr3(t *testing.T) {
	// case 3. maxElems = 10, in Right direction. nil evaluator
	testingKey := makeTestingKeyStr(100)
	ss := SearchSpecifier{searchKey: testingKey, direction: Right,
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
	expectedVal := strHash(testingKey) + 10
	for i := 0; i < len(result); i++ {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElemStr).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal, actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func TestSearchStr4(t *testing.T) {
	// case 4. maxElems = 10, in Left direction.
	testingKey := makeTestingKeyStr(100)
	ss := SearchSpecifier{searchKey: testingKey, direction: Left,
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
	expectedVal := strHash(testingKey) + 10
	for i := len(result) - 1; i >= 0; i-- {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElemStr).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal, actualVal)
			t.FailNow()
		}
		// and decrement by 1.
		expectedVal--
	}
}

func TestSearchStr5(t *testing.T) {
	// case 5. maxElems = 10, in both directions.
	testingKey := makeTestingKeyStr(100)
	ss := SearchSpecifier{searchKey: testingKey, direction: Both,
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
	startKey := makeTestingKeyStr(strHash(testingKey) - 10)
	expectedVal := int(strHash(startKey) + 10)
	for i := 0; i < len(result); i++ {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElemStr).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal, actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func TestSearchStr6(t *testing.T) {

	// case 6. use of evaluator with maxElems = 10, such that
	// evaluator yields more than 10 elements. We should still
	// get 10 elements.

	testingKey := makeTestingKeyStr(100)
	ss := SearchSpecifier{searchKey: testingKey, direction: Both,
		maxElems: 10, evaluator: func(k1 Key, k2 Key) bool {
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
	startKey := makeTestingKeyStr(strHash(testingKey) - 5)
	expectedVal := int(strHash(startKey) + 10)
	for i := 0; i < len(result); i++ {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElemStr).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal, actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func TestSearchStr7(t *testing.T) {

	// case 7. use of evaluator with maxElems = 10, such that
	// evaluator yields less than 10 elements. We should only
	// get less than 10 elements.

	testingKey := makeTestingKeyStr(100)
	ss := SearchSpecifier{searchKey: testingKey, direction: Both,
		maxElems: 10, evaluator: func(k1 Key, k2 Key) bool {
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
	startKey := makeTestingKeyStr(strHash(testingKey) - 2)
	expectedVal := int(strHash(startKey) + 10)
	for i := 0; i < len(result); i++ {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElemStr).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal, actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func TestRemoveStr1(t *testing.T) {
	fmt.Println("Starting remove in order tests")
	for i := 0; i < numElems; i++ {
		key := makeTestingKeyStr(i)
		fmt.Printf("removing key: %v", key)
		err := newtree.Remove(key)
		if err != nil {
			t.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
	}
}

func TestRemoveStr2(t *testing.T) {
	TestInsertStr(t)
	fmt.Println("Starting remove in reverse order tests")
	for i := numElems - 1; i >= 0; i-- {
		key := makeTestingKeyStr(i)
		fmt.Printf("removing key: %v", key)
		err := newtree.Remove(key)
		if err != nil {
			t.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
	}
}

func TestRemoveStr3(t *testing.T) {
	TestInsertStr(t)
	fmt.Println("Starting remove in weird order test")

	removed := make(map[int]bool)
	for i := 0; i < numElems/3; i++ {
		k1 := i
		key := makeTestingKeyStr(k1)
		fmt.Printf("removing key: %v", key)
		err := newtree.Remove(key)
		if err != nil {
			t.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
		removed[k1] = true

		k2 := numElems/3 + i
		key = makeTestingKeyStr(k2)
		fmt.Printf("removing key: %v", key)
		err = newtree.Remove(key)
		if err != nil {
			t.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
		removed[k2] = true

		k3 := numElems*2/3 + i
		key = makeTestingKeyStr(k3)
		fmt.Printf("removing key: %v", key)
		err = newtree.Remove(key)
		if err != nil {
			t.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
		removed[k3] = true
	}

	for i := 0; i < numElems; i++ {
		if !removed[i] {
			newtree.Remove(makeTestingKeyStr(i))
		}
	}
}

func RemoveRoutineStr(t *testing.T, k int, errors []error, index int, wg *sync.WaitGroup) {
	defer wg.Done()
	t.Logf("Removing: %v", k)
	err := newtree.Remove(makeTestingKeyStr(k))
	errors[index] = err
	t.Logf("++++++++++++++++++++++++++++++++")
}

func TestRemoveStr4(t *testing.T) {
	TestInsertStr(t)
	fmt.Println("Starting remove in parallel test")
	errors := make([]error, numElems)
	var wg sync.WaitGroup

	for i := 0; i < numElems; i++ {
		wg.Add(1)
		go RemoveRoutineStr(t, i, errors, i, &wg)
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
