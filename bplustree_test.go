package bplustree

import (
	"fmt"
	"log"
	"math/rand"
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

var numElemChoices = []int{10, 500, 10000}
var degreeChoices = []int{3, 50, 100}

func getNumElemsAndDegreeRandomly() (int, int) {
	idx := rand.Intn(len(numElemChoices))
	return numElemChoices[idx], degreeChoices[idx]
}

func keyEvaluator(k1 Key, k2 Key) bool {
	result := k1.Compare(k2)
	if result < 10 && result > -10 {
		return true
	}

	return false
}
func initTree(t *testing.T, maxDegree int) (*BplusTree, error) {
	ctx := Context{lockMgr: nil, maxDegree: maxDegree}
	// Enable line numbers in logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	newtree, err := NewBplusTree(ctx)
	if err != nil {
		t.Errorf("Failed to create new bplustree: %v", err)
		t.FailNow()
	}
	return newtree, err
}

func insertRoutine(t *testing.T, newtree *BplusTree, elem *TestingElem,
	errors []error, index int, wg *sync.WaitGroup) {
	defer wg.Done()
	t.Logf("Inserting: %v", elem)
	err := newtree.Insert(elem)
	errors[index] = err

	t.Logf("++++++++++++++++++++++++++++++++")
}

func insertInt(t *testing.T, newtree *BplusTree, numElems int) {
	elems := make([]TestingElem, numElems)
	errors := make([]error, numElems)
	var wg sync.WaitGroup

	for i := 0; i < numElems; i++ {
		elem := &elems[i]
		elem.key = TestingKey(i)
		elem.val = i + 10
		wg.Add(1)
		go insertRoutine(t, newtree, elem, errors, i, &wg)
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

func initAndInsert(t *testing.T, maxDegree int, numElems int) (*BplusTree, error) {
	newtree, err := initTree(t, maxDegree)
	insertInt(t, newtree, numElems)
	return newtree, err
}

func search0(t *testing.T, newtree *BplusTree) {
	// case 0. Look for a key which doesn't exist.
	var testingKey TestingKey = 10000000
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

func search1(t *testing.T, newtree *BplusTree, numElems int) {
	// case 1. Exact search.
	testingKey := TestingKey(rand.Intn(numElems))
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

	expectedVal := int(testingKey) + 10
	actualVal := result[0].(*TestingElem).val
	if expectedVal != actualVal {
		t.Errorf("Result doesn't match. expected: %d, got: %d",
			expectedVal, actualVal)
		t.FailNow()
	}
	// case 1.a Exact search. Provide an evaluator and maxElems which
	// should be ignored.
	testingKey1 := TestingKey(rand.Intn(numElems))
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
	expectedVal = int(testingKey1) + 10
	actualVal = result[0].(*TestingElem).val
	if expectedVal != actualVal {
		t.Errorf("Result doesn't match. expected: %d, got: %d",
			expectedVal, actualVal)
		t.FailNow()
	}
}

func search2(t *testing.T, newtree *BplusTree, numElems int) {
	// case 2. maxElems greater than limit.
	testingKey := TestingKey(rand.Intn(numElems))
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

func search3(t *testing.T, newtree *BplusTree, numElems int) {
	// case 3. maxElems = 10, in Right direction. nil evaluator
	if numElems < 11 {
		return
	}
	testingKey := TestingKey(rand.Intn(numElems - 11))
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

	if len(result) != ss.maxElems+1 {
		t.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems,
			len(result))
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

func search4(t *testing.T, newtree *BplusTree, numElems int) {
	// case 4. maxElems = 10, in Left direction.
	if numElems < 11 {
		return
	}
	testingKey := TestingKey(rand.Intn(numElems-11) + 11)
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

func search5(t *testing.T, newtree *BplusTree, numElems int) {
	// case 5. maxElems = 10, in both directions.
	if numElems < 40 {
		return
	}
	testingKey := TestingKey(rand.Intn(numElems-40) + 20)
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

	if len(result) != ss.maxElems+1 {
		t.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems,
			len(result))
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

func search6(t *testing.T, newtree *BplusTree, numElems int) {

	// case 6. use of evaluator with maxElems = 10, such that
	// evaluator yields more than 10 elements. We should still
	// get 10 elements.
	if numElems < 21 {
		return
	}
	testingKey := TestingKey(rand.Intn(numElems-21) + 10)
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

	if len(result) != ss.maxElems+1 {
		t.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems,
			len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	startKey := int(testingKey) - 5
	expectedVal := int(startKey + 10)
	for i := 0; i < len(result); i++ {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElem).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal,
				actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func search7(t *testing.T, newtree *BplusTree, numElems int) {

	// case 7. use of evaluator with maxElems = 10, such that
	// evaluator yields less than 10 elements. We should only
	// get less than 10 elements.
	if numElems < 40 {
		return
	}
	testingKey := TestingKey(rand.Intn(numElems-40) + 20)
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
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal,
				actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func TestInsertAndSearch(t *testing.T) {
	numElems, maxDegree := getNumElemsAndDegreeRandomly()
	newtree, _ := initAndInsert(t, maxDegree, numElems)
	search0(t, newtree)
	search1(t, newtree, numElems)
	search2(t, newtree, numElems)
	search3(t, newtree, numElems)
	search4(t, newtree, numElems)
	search5(t, newtree, numElems)
	search6(t, newtree, numElems)
	search7(t, newtree, numElems)
}

func TestRemove1(t *testing.T) {
	numElems, maxDegree := getNumElemsAndDegreeRandomly()
	newtree, _ := initAndInsert(t, maxDegree, numElems)
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
	// lookup something now and ensures there's nothing.
	testingKey := TestingKey(rand.Intn(numElems))
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err != ErrNotFound || result != nil {
		t.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
}

func TestRemove2(t *testing.T) {
	numElems, maxDegree := getNumElemsAndDegreeRandomly()
	newtree, _ := initAndInsert(t, maxDegree, numElems)
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
	// lookup something now and ensures there's nothing.
	testingKey := TestingKey(rand.Intn(numElems))
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err != ErrNotFound || result != nil {
		t.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
}

func TestRemove3(t *testing.T) {
	numElems, maxDegree := getNumElemsAndDegreeRandomly()
	newtree, _ := initAndInsert(t, maxDegree, numElems)
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

		sKey := rand.Intn(numElems)
		res, serr := newtree.Search(SearchSpecifier{searchKey: TestingKey(sKey),
			direction: Exact})
		if removed[sKey] == true {
			// expect failure
			if serr != ErrNotFound {
				t.Errorf("Expected %v to be not found (res: %v)", sKey, res)
				t.FailNow()
			}
		} else {
			// expect success.
			if err != nil {
				t.Errorf("Expected %v to be found", sKey)
				t.FailNow()
			}
		}

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
	// lookup something now and ensures there's nothing.
	testingKey := TestingKey(rand.Intn(numElems))
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err != ErrNotFound || result != nil {
		t.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}

}

func RemoveRoutine(t *testing.T, newtree *BplusTree, k int, errors []error, index int,
	wg *sync.WaitGroup) {
	defer wg.Done()
	t.Logf("Removing: %v", k)
	err := newtree.Remove(TestingKey(k))
	errors[index] = err
	t.Logf("++++++++++++++++++++++++++++++++")
}

func TestRemove4(t *testing.T) {
	numElems, maxDegree := getNumElemsAndDegreeRandomly()
	newtree, _ := initAndInsert(t, maxDegree, numElems)
	fmt.Println("Starting remove in parallel test")
	errors := make([]error, numElems)
	var wg sync.WaitGroup

	for i := 0; i < numElems; i++ {
		wg.Add(1)
		go RemoveRoutine(t, newtree, i, errors, i, &wg)
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
	// lookup something now and ensures there's nothing.
	testingKey := TestingKey(rand.Intn(numElems))
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err != ErrNotFound || result != nil {
		t.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
}

func initTreeStr(t *testing.T, maxDegree int) (*BplusTree, error) {
	ctx := Context{lockMgr: nil, maxDegree: maxDegree}
	var err error
	// Enable line numbers in logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	newtree, err := NewBplusTree(ctx)
	if err != nil {
		t.Errorf("Failed to create new bplustree: %v", err)
		t.FailNow()
	}
	return newtree, err
}

func insertRoutineStr(t *testing.T, newtree *BplusTree, elem *TestingElemStr,
	errors []error, index int, wg *sync.WaitGroup) {
	defer wg.Done()
	t.Logf("Inserting: %v", elem)
	err := newtree.Insert(elem)
	errors[index] = err

	t.Logf("++++++++++++++++++++++++++++++++")
}

func makeTestingKeyStr(k int) TestingKeyStr {
	return TestingKeyStr(fmt.Sprintf("key_%d", k))
}

func insertIntStr(t *testing.T, newtree *BplusTree, numElems int) {
	t.Log("Starting insert test..")
	elems := make([]TestingElemStr, numElems)
	errors := make([]error, numElems)
	var wg sync.WaitGroup

	for i := 0; i < numElems; i++ {
		elem := &elems[i]
		elem.key = makeTestingKeyStr(i)
		elem.val = i + 10
		wg.Add(1)
		go insertRoutineStr(t, newtree, elem, errors, i, &wg)
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

func initAndInsertStr(t *testing.T, maxDegree int, numElems int) (*BplusTree, error) {
	newtree, err := initTreeStr(t, maxDegree)
	insertIntStr(t, newtree, numElems)
	return newtree, err
}

func searchStr0(t *testing.T, newtree *BplusTree) {
	t.Log("Starting search tests..")

	// case 0. Look for a key which doesn't exist.
	testingKey := makeTestingKeyStr(1000000)
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
func searchStr1(t *testing.T, newtree *BplusTree, numElems int) {
	// case 1. Exact search.
	testingKey := makeTestingKeyStr(rand.Intn(numElems))
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
	expectedVal := int(strHash(testingKey)) + 10
	actualVal := result[0].(*TestingElemStr).val
	if expectedVal != actualVal {
		t.Errorf("Result doesn't match. expected: %d, got: %d",
			expectedVal, actualVal)
		t.FailNow()
	}

	// case 1.a Exact search. Provide an evaluator and maxElems which
	// should be ignored.
	testingKey1 := makeTestingKeyStr(rand.Intn(numElems))
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
	expectedVal = int(strHash(testingKey1)) + 10
	actualVal = result[0].(*TestingElemStr).val
	if expectedVal != actualVal {
		t.Errorf("Result doesn't match. expected: %d, got: %d",
			expectedVal, actualVal)
		t.FailNow()
	}
}

func searchStr2(t *testing.T, newtree *BplusTree, numElems int) {
	// case 2. maxElems greater than limit.
	testingKey := makeTestingKeyStr(rand.Intn(numElems))
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

func searchStr3(t *testing.T, newtree *BplusTree, numElems int) {
	// case 3. maxElems = 10, in Right direction. nil evaluator
	if numElems < 11 {
		return
	}
	testingKey := makeTestingKeyStr(rand.Intn(numElems - 11))
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

	if len(result) != ss.maxElems+1 {
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

func searchStr4(t *testing.T, newtree *BplusTree, numElems int) {
	// case 4. maxElems = 10, in Left direction.
	if numElems < 11 {
		return
	}
	testingKey := makeTestingKeyStr(rand.Intn(numElems-11) + 11)
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

	if len(result) != ss.maxElems+1 {
		t.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems,
			len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	expectedVal := strHash(testingKey) + 10
	for i := len(result) - 1; i >= 0; i-- {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElemStr).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal,
				actualVal)
			t.FailNow()
		}
		// and decrement by 1.
		expectedVal--
	}
}

func searchStr5(t *testing.T, newtree *BplusTree, numElems int) {
	// case 5. maxElems = 10, in both directions.
	if numElems < 41 {
		return
	}
	testingKey := makeTestingKeyStr(rand.Intn(numElems-41) + 20)
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

	if len(result) != ss.maxElems+1 {
		t.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems,
			len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	startKey := makeTestingKeyStr(strHash(testingKey) - 10)
	expectedVal := int(strHash(startKey) + 10)
	for i := 0; i < len(result); i++ {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElemStr).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal,
				actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func searchStr6(t *testing.T, newtree *BplusTree, numElems int) {

	// case 6. use of evaluator with maxElems = 10, such that
	// evaluator yields more than 10 elements. We should still
	// get 10 elements.
	if numElems < 41 {
		return
	}
	testingKey := makeTestingKeyStr(rand.Intn(numElems-41) + 20)
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

	if len(result) != ss.maxElems+1 {
		t.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems,
			len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	startKey := makeTestingKeyStr(strHash(testingKey) - 5)
	expectedVal := int(strHash(startKey) + 10)
	for i := 0; i < len(result); i++ {
		//fmt.Println(reflect.TypeOf(result[i]))
		actualVal := result[i].(*TestingElemStr).val
		if actualVal != expectedVal {
			t.Errorf("At index %d: expected: %d, got %v", i, expectedVal,
				actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func searchStr7(t *testing.T, newtree *BplusTree, numElems int) {

	// case 7. use of evaluator with maxElems = 10, such that
	// evaluator yields less than 10 elements. We should only
	// get less than 10 elements.
	if numElems < 41 {
		return
	}
	testingKey := makeTestingKeyStr(rand.Intn(numElems-41) + 20)
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

func TestInsertAndSearchStr(t *testing.T) {
	numElems, maxDegree := getNumElemsAndDegreeRandomly()
	newtree, _ := initAndInsertStr(t, maxDegree, numElems)
	searchStr0(t, newtree)
	searchStr1(t, newtree, numElems)
	searchStr2(t, newtree, numElems)
	searchStr3(t, newtree, numElems)
	searchStr4(t, newtree, numElems)
	searchStr5(t, newtree, numElems)
	searchStr6(t, newtree, numElems)
	searchStr7(t, newtree, numElems)
}

func TestRemoveStr1(t *testing.T) {
	numElems, maxDegree := getNumElemsAndDegreeRandomly()
	newtree, _ := initAndInsertStr(t, maxDegree, numElems)
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

	// lookup something now and ensures there's nothing.
	testingKey := makeTestingKeyStr(rand.Intn(numElems))
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err != ErrNotFound || result != nil {
		t.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
}

func TestRemoveStr2(t *testing.T) {
	numElems, maxDegree := getNumElemsAndDegreeRandomly()
	newtree, _ := initAndInsertStr(t, maxDegree, numElems)
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
	// lookup something now and ensures there's nothing.
	testingKey := makeTestingKeyStr(rand.Intn(numElems))
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err != ErrNotFound || result != nil {
		t.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
}

func TestRemoveStr3(t *testing.T) {
	numElems, maxDegree := getNumElemsAndDegreeRandomly()
	newtree, _ := initAndInsertStr(t, maxDegree, numElems)
	fmt.Println("Starting remove in weird order test")

	removed := make(map[int]bool)
	for i := 0; i < numElems/3; i++ {
		k1 := i
		key := makeTestingKeyStr(k1)
		fmt.Printf("1. Removing key: %v\n", key)
		err := newtree.Remove(key)
		if err != nil {
			t.Errorf("Failed to remove key: %v got %v", key, err)
			fmt.Printf("removed so far: %v\n", removed)
			t.FailNow()
		}
		removed[k1] = true

		k2 := numElems/3 + i
		key = makeTestingKeyStr(k2)
		fmt.Printf("2. Removing key: %v\n", key)
		err = newtree.Remove(key)
		if err != nil {
			t.Errorf("Failed to remove key: %v got %v", key, err)
			fmt.Printf("removed so far: %v", removed)
			t.FailNow()
		}
		removed[k2] = true

		sKey := makeTestingKeyStr(rand.Intn(numElems))
		intval := strHash(sKey)
		res, serr := newtree.Search(SearchSpecifier{searchKey: sKey,
			direction: Exact})

		if removed[intval] == true {
			// expect failure
			if serr != ErrNotFound {
				t.Errorf("Expected %v to be not found (res: %v)", sKey, res)
				t.FailNow()
			}
		} else {
			// expect success.
			if err != nil {
				t.Errorf("Expected %v to be found\n", sKey)
				t.FailNow()
			}
		}

		k3 := numElems - 1 - i
		key = makeTestingKeyStr(k3)
		fmt.Printf("3. Removing key: %v\n", key)
		err = newtree.Remove(key)
		if err != nil {
			t.Errorf("Failed to remove key: %v got %v", key, err)
			fmt.Printf("removed so far: %v\n", removed)
			t.FailNow()
		}
		removed[k3] = true
	}

	for i := 0; i < numElems; i++ {
		if !removed[i] {
			newtree.Remove(makeTestingKeyStr(i))
		}
	}
	// lookup something now and ensures there's nothing.
	testingKey := makeTestingKeyStr(rand.Intn(numElems))
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err != ErrNotFound || result != nil {
		t.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
}

func removeRoutineStr(t *testing.T, newtree *BplusTree, k int, errors []error,
	index int, wg *sync.WaitGroup, mux *sync.RWMutex, removed *map[int]bool) {
	defer wg.Done()
	t.Logf("Removing: %v", k)
	mux.Lock()
	err := newtree.Remove(makeTestingKeyStr(k))
	(*removed)[k] = true
	mux.Unlock()
	errors[index] = err
	t.Logf("++++++++++++++++++++++++++++++++")
}

func searchRoutineStr(t *testing.T, newtree *BplusTree, numElems int,
	wg *sync.WaitGroup, mux *sync.RWMutex, removed *map[int]bool) {

	defer wg.Done()
	for i := 0; i < numElems; i++ {
		mux.Lock()
		k := rand.Intn(numElems)
		fmt.Printf("Searching: %v\n", k)
		testingKey := makeTestingKeyStr(k)
		ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
		result, err := newtree.Search(ss)
		if (*removed)[k] {
			if err != ErrNotFound || result != nil {
				t.Errorf("Expected to not find the key: %v", ss.searchKey)
				t.FailNow()
			}
		}
		if !(*removed)[k] {
			if err != nil || result == nil {
				t.Errorf("Expected to find the key: %v, but encountered err: %v\n",
					ss.searchKey, err)
				t.FailNow()
			}

			expectedVal := k + 10
			actualVal := result[0].(*TestingElemStr).val
			if actualVal != expectedVal {
				t.Errorf("Expected %d, got %d for key: %v", expectedVal, actualVal,
					ss.searchKey)
				t.FailNow()
			}
		}
		mux.Unlock()
		t.Logf("++++++++++++++++++++++++++++++++")
	}
}

func TestRemoveStr4(t *testing.T) {
	numElems, maxDegree := getNumElemsAndDegreeRandomly()
	newtree, _ := initAndInsertStr(t, maxDegree, numElems)
	fmt.Println("Starting remove in parallel test")
	errors := make([]error, numElems)
	var wg, wgSearch sync.WaitGroup

	removed := make(map[int]bool)
	var mux sync.RWMutex

	wgSearch.Add(1)
	go searchRoutineStr(t, newtree, numElems, &wgSearch, &mux, &removed)

	for i := 0; i < numElems; i++ {
		wg.Add(1)
		go removeRoutineStr(t, newtree, i, errors, i, &wg, &mux, &removed)
	}

	wg.Wait()
	for i := 0; i < numElems; i++ {
		if errors[i] != nil {
			t.Errorf("Failed to insert: %d: %v", i, errors[i])
			t.FailNow()
		}
	}
	wgSearch.Wait()

	t.Logf("\nFinal tree:\n")
	newtree.Print()
	// lookup something now and ensures there's nothing.
	testingKey := makeTestingKeyStr(rand.Intn(numElems))
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err != ErrNotFound || result != nil {
		t.Errorf("Expected to not find the key: %v, err: %v, result: %v",
			ss.searchKey, err, result)
		t.FailNow()
	}
}
