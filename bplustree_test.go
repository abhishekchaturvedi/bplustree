// Use of this software is governed by an Apache 2.0
// licence which can be found in the license file

package bplustree

import (
	"bplustree/common"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/golang/glog"
)

var seededRandTest *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

type TestingElem struct {
	TKey common.Key `json:"tkey"`
	Val  Element    `json:"val"`
}

func makeTestingKey(k int) common.Key {
	return common.Key{BPTkey: fmt.Sprintf("%d", k)}
}

func getTestingKeyVal(k common.Key) int {
	var kval int
	fmt.Sscanf(k.BPTkey, "%d", &kval)
	return kval
}

func makeTestingVal(v int) Element {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(v))
	e := Element{}
	copy(e.Buffer[:], b)
	return e
}
func getTestingVal(elem Element) int {
	return int(binary.LittleEndian.Uint32(elem.Buffer[:]))
}

var numElemChoices = []int{20, 500, 10000}
var degreeChoices = []int{3, 50, 100}

func getNumNodesAndLevels(degree int, numElems int) (int, int) {
	nnl := numElems
	numnodes := 0
	nlevels := 0
	for {
		nlevels++
		nnl = nnl / ((degree / 2) + 1)
		numnodes += nnl
		if nnl <= 1 {
			numnodes++
			break
		}
	}
	return numnodes, nlevels
}

func getNumElemsAndDegreeRandomly() (int, int, int, int) {
	idx := seededRandTest.Intn(len(numElemChoices))
	numElems := numElemChoices[idx]
	degree := degreeChoices[idx]
	numnodes, nlevels := getNumNodesAndLevels(degree, numElems)
	glog.Infof("Will use: numElems: %d, degree: %d, numNodes: %d, levels:%d\n",
		numElems, degree, numnodes, nlevels)
	return numElemChoices[idx], degreeChoices[idx], numnodes, nlevels
}

func keyEvaluator(k1 common.Key, k2 common.Key) bool {
	result := k1.Compare(k2)
	if result < 10 && result > -10 {
		return true
	}

	return false
}

func initTree(t *testing.T, ctx Context) (*BplusTree, error) {
	newtree, err := NewBplusTree(ctx)
	if err != nil {
		glog.Errorf("Failed to create new bplustree: %v", err)
		t.FailNow()
	}
	return newtree, err
}

func insertRoutine(t *testing.T, newtree *BplusTree, elem *TestingElem,
	errors []error, index int, wg *sync.WaitGroup) {
	defer wg.Done()
	err := newtree.Insert(elem.TKey, elem.Val)
	if err != nil {
		glog.Infof("failed to insert: %v (err: %v)", elem, err)
	}
	errors[index] = err
}

func getMMParams(newtree *BplusTree) string {
	return newtree.context.memMgr.Policy()
}

func insertIntThreaded(t *testing.T, newtree *BplusTree, numElems int) []TestingElem {
	elems := make([]TestingElem, numElems)
	errors := make([]error, numElems)
	var wg sync.WaitGroup
	for i := 0; i < numElems; i++ {
		elem := &elems[i]
		elem.TKey = makeTestingKey(i)
		elem.Val = makeTestingVal(i + 10)
		wg.Add(1)
		go insertRoutine(t, newtree, elem, errors, i, &wg)
	}

	wg.Wait()
	policy := getMMParams(newtree)
	elemsInserted := make([]TestingElem, 0)
	for i := 0; i < numElems; i++ {
		if errors[i] != nil {
			glog.Errorf("failed to insert: %v: %v", elems[i], errors[i])
			if policy != common.MemMgrPolicyLocalLRU {
				glog.Errorf("Failed to insert: %d: %v", i, errors[i])
				t.FailNow()
			}
		} else {
			elemsInserted = append(elemsInserted, elems[i])
		}
	}
	return elemsInserted
}

func initAndInsert(t *testing.T, ctx Context, numElems int) (*BplusTree,
	[]TestingElem, error) {

	defer glog.Flush()
	glog.Infof("Initializing tree with: %v", ctx)
	newtree, err := initTree(t, ctx)
	glog.Infof("Initiating insert of %d elements", numElems)
	elemsInserted := insertIntThreaded(t, newtree, numElems)
	return newtree, elemsInserted, err
}

func search0(t *testing.T, newtree *BplusTree, elemsInserted []TestingElem) {
	// case 0. Look for a key which doesn't exist.
	testingKey := makeTestingKey(10000000)
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	glog.Infof("search0: Searching for: %v", ss)
	result, err := newtree.Search(ss)
	if err == nil {
		glog.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result != nil {
		glog.Errorf("Expected to not get any result but got: %v", result)
		t.FailNow()
	}
}

func search1(t *testing.T, newtree *BplusTree, numElems int, elemsInserted []TestingElem) {
	// case 1. Exact search.
	testingKeyVal := seededRandTest.Intn(numElems)
	testingKey := makeTestingKey(testingKeyVal)
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	glog.Infof("search1: Searching for: %v", ss)
	result, err := newtree.Search(ss)
	if err != nil {
		glog.Errorf("Failed find the key: %v, err: %v", ss.searchKey, err)
		t.FailNow()
	}
	if result == nil {
		glog.Errorf("Got nil result")
		t.FailNow()
	}

	expectedVal := int(testingKeyVal) + 10
	actualVal := getTestingVal(result[0])
	if expectedVal != actualVal {
		glog.Errorf("Result doesn't match. expected: %d, got: %d",
			expectedVal, actualVal)
		t.FailNow()
	}
	// case 1.a Exact search. Provide an evaluator and maxElems which
	// should be ignored.
	testingKey1Val := seededRandTest.Intn(numElems)
	testingKey1 := makeTestingKey(testingKey1Val)
	ss = SearchSpecifier{searchKey: testingKey1, direction: Exact,
		maxElems: 50, evaluator: keyEvaluator}
	glog.Infof("search1: Searching for: %v", ss)
	result, err = newtree.Search(ss)
	if len(result) > 1 {
		glog.Errorf("Expected to find exact match, but got more (%d)", len(result))
		t.FailNow()
	}
	if err != nil {
		glog.Errorf("Expected to find the key: %v (err: %v)", ss.searchKey, err)
		t.FailNow()
	}
	if result == nil {
		glog.Errorf("Got nil result")
		t.FailNow()
	}
	expectedVal = int(testingKey1Val) + 10
	actualVal = getTestingVal(result[0])
	if expectedVal != actualVal {
		glog.Errorf("Result doesn't match. expected: %d, got: %d",
			expectedVal, actualVal)
		t.FailNow()
	}
}

func search2(t *testing.T, newtree *BplusTree, numElems int, elemsInserted []TestingElem) {
	// case 2. maxElems greater than limit.
	testingKeyVal := seededRandTest.Intn(numElems)
	testingKey := makeTestingKey(testingKeyVal)
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact,
		maxElems: 100, evaluator: keyEvaluator}
	glog.Infof("search2: Searching for: %v", ss)
	result, err := newtree.Search(ss)
	if err == nil {
		glog.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result != nil {
		glog.Errorf("Expected to not get any result but got: %v", result)
		t.FailNow()
	}
}

func search3(t *testing.T, newtree *BplusTree, numElems int, elemsInserted []TestingElem) {
	// case 3. maxElems = 10, in Right direction. nil evaluator
	if numElems < 11 {
		return
	}
	testingKeyVal := seededRandTest.Intn(numElems - 11)
	testingKey := makeTestingKey(testingKeyVal)
	ss := SearchSpecifier{searchKey: testingKey, direction: Right,
		maxElems: 10}
	glog.Infof("search3: Searching for: %v", ss)
	result, err := newtree.Search(ss)
	if err != nil {
		glog.Errorf("Expected to find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result == nil {
		glog.Errorf("Expected to get result but nil: %v", result)
		t.FailNow()
	}

	if len(result) != ss.maxElems+1 {
		glog.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems,
			len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	expectedVal := int(testingKeyVal + 10)
	for i := 0; i < len(result); i++ {
		//glog.Info(reflect.TypeOf(result[i]))
		actualVal := getTestingVal(result[i])
		if actualVal != expectedVal {
			glog.Errorf("At index %d: expected: %d, got %v", i, expectedVal, actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func search4(t *testing.T, newtree *BplusTree, numElems int, elemsInserted []TestingElem) {
	// case 4. maxElems = 10, in Left direction.
	if numElems < 11 {
		return
	}
	testingKeyVal := seededRandTest.Intn(numElems-11) + 11
	testingKey := makeTestingKey(testingKeyVal)
	ss := SearchSpecifier{searchKey: testingKey, direction: Left,
		maxElems: 10}
	glog.Infof("search4: Searching for: %v", ss)
	result, err := newtree.Search(ss)
	if err != nil {
		glog.Errorf("Expected to find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result == nil {
		glog.Errorf("Expected to get result but nil: %v", result)
		t.FailNow()
	}

	if len(result) < ss.maxElems || len(result) > ss.maxElems+1 {
		glog.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems, len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	expectedVal := int(testingKeyVal + 10)
	for i := len(result) - 1; i >= 0; i-- {
		//glog.Info(reflect.TypeOf(result[i]))
		actualVal := getTestingVal(result[i])
		if actualVal != expectedVal {
			glog.Errorf("At index %d: expected: %d, got %v", i, expectedVal, actualVal)
			t.FailNow()
		}
		// and decrement by 1.
		expectedVal--
	}
}

func search5(t *testing.T, newtree *BplusTree, numElems int, elemsInserted []TestingElem) {
	// case 5. maxElems = 10, in both directions.
	if numElems < 40 {
		return
	}
	testingKeyVal := seededRandTest.Intn(numElems-40) + 20
	testingKey := makeTestingKey(testingKeyVal)
	ss := SearchSpecifier{searchKey: testingKey, direction: Both,
		maxElems: 20}
	glog.Infof("search5: Searching for: %v", ss)
	result, err := newtree.Search(ss)
	if err != nil {
		glog.Errorf("Expected to find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result == nil {
		glog.Errorf("Expected to get result but nil: %v", result)
		t.FailNow()
	}

	if len(result) != ss.maxElems+1 {
		glog.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems,
			len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	startKey := int(testingKeyVal) - 10
	expectedVal := int(startKey + 10)
	for i := 0; i < len(result); i++ {
		actualVal := getTestingVal(result[i])
		if actualVal != expectedVal {
			glog.Errorf("At index %d: expected: %d, got %v", i, expectedVal, actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func search6(t *testing.T, newtree *BplusTree, numElems int, elemsInserted []TestingElem) {

	// case 6. use of evaluator with maxElems = 10, such that
	// evaluator yields more than 10 elements. We should still
	// get 10 elements.
	if numElems < 21 {
		return
	}
	testingKeyVal := seededRandTest.Intn(numElems-21) + 10
	testingKey := makeTestingKey(testingKeyVal)
	ss := SearchSpecifier{searchKey: testingKey, direction: Both,
		maxElems: 10, evaluator: func(k1 common.Key, k2 common.Key) bool {
			result := k1.Compare(k2)
			if result < 10 && result > -10 {
				return true
			}

			return false
		},
	}
	glog.Infof("search6: Searching for: %v", ss)
	result, err := newtree.Search(ss)
	if err != nil {
		glog.Errorf("Expected to find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result == nil {
		glog.Errorf("Expected to get result but nil: %v", result)
		t.FailNow()
	}

	if len(result) != ss.maxElems+1 {
		glog.Errorf("Expected to find atleast %d results, got: %d", ss.maxElems,
			len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	startKey := int(testingKeyVal) - 5
	expectedVal := int(startKey + 10)
	for i := 0; i < len(result); i++ {
		//glog.Info(reflect.TypeOf(result[i]))
		actualVal := getTestingVal(result[i])
		if actualVal != expectedVal {
			glog.Errorf("At index %d: expected: %d, got %v", i, expectedVal,
				actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func search7(t *testing.T, newtree *BplusTree, numElems int, elemsInserted []TestingElem) {

	// case 7. use of evaluator with maxElems = 10, such that
	// evaluator yields less than 10 elements. We should only
	// get less than 10 elements.
	if numElems < 40 {
		return
	}
	testingKeyVal := seededRandTest.Intn(numElems-40) + 20
	testingKey := makeTestingKey(testingKeyVal)
	ss := SearchSpecifier{searchKey: testingKey, direction: Both,
		maxElems: 10, evaluator: func(k1 common.Key, k2 common.Key) bool {
			result := k1.Compare(k2)
			if result < 3 && result > -3 {
				return true
			}

			return false
		},
	}
	glog.Infof("search7: Searching for: %v", ss)
	result, err := newtree.Search(ss)
	if err != nil {
		glog.Errorf("Expected to find the key: %v", ss.searchKey)
		t.FailNow()
	}
	if result == nil {
		glog.Errorf("Expected to get result but nil: %v", result)
		t.FailNow()
	}

	if len(result) != 5 {
		glog.Errorf("Expected to find 5 results, got: %d", len(result))
		t.FailNow()
	}

	// Note that our values are +10 of the key.
	startKey := int(testingKeyVal) - 2
	expectedVal := int(startKey + 10)
	for i := 0; i < len(result); i++ {
		//glog.Info(reflect.TypeOf(result[i]))
		actualVal := getTestingVal(result[i])
		if actualVal != expectedVal {
			glog.Errorf("At index %d: expected: %d, got %v", i, expectedVal,
				actualVal)
			t.FailNow()
		}
		// and increment by 1.
		expectedVal++
	}
}

func setupParams(policy string, dbpolicy string) (*Context, int, int, int) {
	if policy != common.MemMgrPolicyLocalMap &&
		(dbpolicy != common.DBMgrPolicyLocalMap && dbpolicy != common.DBMgrPolicyADB) {
		glog.Errorf("Invalid policy: '%s', '%s'", policy, dbpolicy)
		return nil, 0, 0, 0
	}
	numElems, maxDegree, numnodes, nlevels := getNumElemsAndDegreeRandomly()
	limit := numnodes / 2
	kt := common.KeyType(common.OrderedStrType)
	pfx := policy
	var mm MemMgr
	if policy == common.MemMgrPolicyLocalLRU {
		mm = NewLocalLRUMemMgr(limit, kt, pfx)
	} else if policy == common.MemMgrPolicyLocalMap {
		mm = NewLocalHashMapMemMgr(kt, pfx)
	} else {
		glog.Errorf("undefined policy: %s", policy)
		panic("unsupported policy")
	}
	var dbm DBMgr
	var err error
	if dbpolicy == common.DBMgrPolicyADB {
		dbm, err = NewArangoDBMgr("http://localhost:8529", "bptest", "root", "root",
			"bplustree-roots", "tree_1_root")
		if err != nil {
			glog.Errorf("failed to connect: %v", err)
			return nil, 0, 0, 0
		}
	} else {
		dbm = NewLocalHashMapDBMgr(dbpolicy)
	}
	ctx := Context{lockMgr: nil, maxDegree: maxDegree, memMgr: mm, keyType: kt,
		pfx: pfx, dbMgr: dbm}
	return &ctx, numElems, nlevels, numnodes
}

func TestInsertAndSearchLocalLRU(t *testing.T) {
	InsertAndSearch(t, common.MemMgrPolicyLocalLRU, common.DBMgrPolicyLocalMap)
}

func TestRemoveLRU1(t *testing.T) {
	Remove1(t, common.MemMgrPolicyLocalLRU, common.DBMgrPolicyLocalMap)
}

func TestRemoveLRU2(t *testing.T) {
	Remove2(t, common.MemMgrPolicyLocalLRU, common.DBMgrPolicyLocalMap)
}

func TestRemoveLRU3(t *testing.T) {
	Remove3(t, common.MemMgrPolicyLocalLRU, common.DBMgrPolicyLocalMap)
}

func TestRemoveLRU4(t *testing.T) {
	Remove4(t, common.MemMgrPolicyLocalLRU, common.DBMgrPolicyLocalMap)
}

func InsertAndSearch(t *testing.T, mmPolicy string, dbmPolicy string) {
	defer glog.Flush()
	ctx, numElems, _, _ := setupParams(mmPolicy, dbmPolicy)
	newtree, elemsInserted, _ := initAndInsert(t, *ctx, numElems)

	search0(t, newtree, elemsInserted)
	search1(t, newtree, numElems, elemsInserted)
	search2(t, newtree, numElems, elemsInserted)
	// since we are now using string keys with fnv hash we can't gurantee
	// that the elems will be ordered the way we want, so verification is
	// not deterministic anymore. Commenting the two tests for now.
	//search3(t, newtree, numElems, elemsInserted)
	//search4(t, newtree, numElems, elemsInserted)
	//search5(t, newtree, numElems, elemsInserted)
	//search6(t, newtree, numElems, elemsInserted)
	//search7(t, newtree, numElems, elemsInserted)
}

func Remove1(t *testing.T, mmPolicy string, dbmPolicy string) {
	ctx, numElems, _, _ := setupParams(mmPolicy, dbmPolicy)
	newtree, _, _ := initAndInsert(t, *ctx, numElems)
	glog.Info("Starting remove in order tests")
	for i := 0; i < numElems; i++ {
		key := makeTestingKey(i)
		glog.Infof("removing key: %v", key)
		err := newtree.Remove(key)
		if err != nil {
			glog.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
	}
	// lookup something now and ensures there's nothing.
	testingKey := makeTestingKey(seededRandTest.Intn(numElems))
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err != common.ErrNotFound || result != nil {
		glog.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
}

func Remove2(t *testing.T, mmPolicy string, dbmPolicy string) {
	ctx, numElems, _, _ := setupParams(mmPolicy, dbmPolicy)
	newtree, _, _ := initAndInsert(t, *ctx, numElems)
	glog.Info("Starting remove in reverse order tests")
	for i := numElems - 1; i >= 0; i-- {
		key := makeTestingKey(i)
		glog.Infof("removing key: %v", key)
		err := newtree.Remove(key)
		if err != nil {
			glog.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
	}
	// lookup something now and ensures there's nothing.
	testingKey := makeTestingKey(seededRandTest.Intn(numElems))
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err != common.ErrNotFound || result != nil {
		glog.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
}

func Remove3(t *testing.T, mmPolicy string, dbmPolicy string) {
	ctx, numElems, _, _ := setupParams(mmPolicy, dbmPolicy)
	newtree, _, _ := initAndInsert(t, *ctx, numElems)
	glog.Info("Starting remove in weird order test")

	removed := make(map[int]bool)
	for i := 0; i < numElems/3; i++ {
		k1 := i
		key := makeTestingKey(k1)
		glog.Infof("removing key: %v", key)
		err := newtree.Remove(key)
		if err != nil {
			glog.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
		removed[k1] = true

		sKey := seededRandTest.Intn(numElems)
		res, serr := newtree.Search(SearchSpecifier{searchKey: makeTestingKey(sKey),
			direction: Exact})
		if removed[sKey] == true {
			// expect failure
			if serr != common.ErrNotFound {
				glog.Errorf("Expected %v to be not found (res: %v)", sKey, res)
				t.FailNow()
			}
		} else {
			// expect success.
			if err != nil {
				glog.Errorf("Expected %v to be found", sKey)
				t.FailNow()
			}
		}

		k2 := numElems/3 + i
		key = makeTestingKey(k2)
		glog.Infof("removing key: %v", key)
		err = newtree.Remove(key)
		if err != nil {
			glog.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
		removed[k2] = true

		k3 := numElems*2/3 + i
		key = makeTestingKey(k3)
		glog.Infof("removing key: %v", key)
		err = newtree.Remove(key)
		if err != nil {
			glog.Errorf("Failed to remove key: %v got %v", key, err)
			t.FailNow()
		}
		removed[k3] = true
	}

	for i := 0; i < numElems; i++ {
		if !removed[i] {
			newtree.Remove(makeTestingKey(i))
		}
	}
	// lookup something now and ensures there's nothing.
	testingKey := makeTestingKey(seededRandTest.Intn(numElems))
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err != common.ErrNotFound || result != nil {
		glog.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
}

func RemoveRoutine(t *testing.T, newtree *BplusTree, k int, errors []error, index int,
	wg *sync.WaitGroup) {
	defer wg.Done()
	//t.Logf("Removing: %v", k)
	err := newtree.Remove(makeTestingKey(k))
	if err != nil {
		glog.Infof("failed to remove %v (err: %v)", makeTestingKey(k), err)
	}
	errors[index] = err
}

func Remove4(t *testing.T, mmPolicy string, dbmPolicy string) {
	ctx, numElems, _, _ := setupParams(mmPolicy, dbmPolicy)
	newtree, _, _ := initAndInsert(t, *ctx, numElems)
	glog.Info("Starting remove in parallel test")
	errors := make([]error, numElems)
	var wg sync.WaitGroup

	for i := 0; i < numElems; i++ {
		wg.Add(1)
		go RemoveRoutine(t, newtree, i, errors, i, &wg)
	}

	wg.Wait()
	for i := 0; i < numElems; i++ {
		if errors[i] != nil {
			glog.Errorf("Failed to remove: %d: %v", i, errors[i])
			t.FailNow()
		}
	}

	// lookup something now and ensures there's nothing.
	testingKey := makeTestingKey(seededRandTest.Intn(numElems))
	ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
	result, err := newtree.Search(ss)
	if err != common.ErrNotFound || result != nil {
		glog.Errorf("Expected to not find the key: %v", ss.searchKey)
		t.FailNow()
	}
}

func TestInsertAndSearchLocalLRUWithFailures(t *testing.T) {
	defer glog.Flush()
	ctx, numElems, nlevels, _ := setupParams(common.MemMgrPolicyLocalLRU, common.DBMgrPolicyLocalMap)
	glog.Infof("enabling testpoint %v with freq of %d", testPointFailDBFetch, nlevels-1)
	TestPointEnable(testPointFailDBFetch, nlevels-1)
	glog.Infof("enabling testpoint %v with freq of %d", testPointFailDBUpdate, numElems/3)
	TestPointEnable(testPointFailDBUpdate, numElems/3)

	newtree, elemsInserted, _ := initAndInsert(t, *ctx, numElems)
	glog.Infof("resetting testpoint %v", testPointFailDBFetch)
	// Can't have test point enabled during search otherwise search will fail.
	TestPointResetAll()
	for _, elem := range elemsInserted {
		glog.Infof("searching for elem: %v", elem)
		_, serr := newtree.Search(SearchSpecifier{searchKey: elem.TKey,
			direction: Exact})
		if serr != nil {
			glog.Errorf("failed to find elem/key: %v", elem)
			glog.Flush()
			t.FailNow()
		}
	}
}

func RemoveWithFailure(t *testing.T, newtree *BplusTree, numElems int,
	elemsInserted []TestingElem) map[common.Key]bool {

	glog.Info("Starting remove with failure test")
	errors := make([]error, numElems)
	elemsRemoved := make(map[common.Key]bool)
	var wg sync.WaitGroup

	for _, elem := range elemsInserted {
		wg.Add(1)
		index := getTestingKeyVal(elem.TKey)
		go RemoveRoutine(t, newtree, index, errors, index, &wg)
	}

	wg.Wait()
	for _, elem := range elemsInserted {
		errIndex := getTestingKeyVal(elem.TKey)
		glog.V(2).Infof("checking result of removal of %v (err: %v)",
			elem, errors[errIndex])
		if errors[errIndex] != nil {
			// Let's retry removal here again.
			glog.Infof("retrying removal of %v (earlier error:%v", elem, errors[errIndex])
			err := newtree.Remove(elem.TKey)
			if err != nil {
				glog.Errorf("retry of removal of %v failed (err: %v)", elem, err)
			} else {
				// successfully removed
				elemsRemoved[elem.TKey] = true
			}
		} else {
			// successfully removed
			elemsRemoved[elem.TKey] = true
		}
	}
	return elemsRemoved
}

func TestInsertRemoveSearchLRUWithFailures(t *testing.T) {
	defer glog.Flush()
	ctx, numElems, nlevels, _ := setupParams(common.MemMgrPolicyLocalLRU, common.DBMgrPolicyLocalMap)
	freq := nlevels - 1
	glog.Infof("enabling testpoint %v with freq of %d", testPointFailDBFetch, freq)
	TestPointEnable(testPointFailDBFetch, freq)
	glog.Infof("enabling testpoint %v with freq of %d", testPointFailDBUpdate, numElems/3)
	TestPointEnable(testPointFailDBUpdate, numElems/3)
	newtree, elemsInserted, _ := initAndInsert(t, *ctx, numElems)

	glog.Infof("Done inserting..")
	glog.Infof("starting..remove")

	elemsRemoved := RemoveWithFailure(t, newtree, numElems, elemsInserted)
	// Can't have test point enabled during search otherwise search will fail.
	glog.Infof("resetting all testpoints")
	TestPointResetAll()
	newtree.Print()
	newtree.context.memMgr.Print()
	newtree.context.dbMgr.LogAllKeys()

	glog.Infof("Searching now")
	for _, elem := range elemsInserted {
		testingKey := elem.TKey
		ss := SearchSpecifier{searchKey: testingKey, direction: Exact}
		result, err := newtree.Search(ss)
		_, ok := elemsRemoved[testingKey]
		if ok {
			if err != common.ErrNotFound {
				glog.Errorf("expected to not find the key: %v (err: %v)", ss.searchKey, err)
				newtree.Print()
				newtree.context.memMgr.Print()
				newtree.context.dbMgr.LogAllKeys()
				t.FailNow()
			}
		} else {
			if err != nil {
				glog.Errorf("expected to find key: %v", ss.searchKey)
				newtree.Print()
				newtree.context.memMgr.Print()
				newtree.context.dbMgr.LogAllKeys()
				t.FailNow()
			}
			expectedVal := getTestingKeyVal(testingKey) + 10
			actualVal := getTestingVal(result[0])
			if actualVal != expectedVal {
				glog.Errorf("result mismatch. expected: %v, got: %v", expectedVal, actualVal)
				newtree.Print()
				newtree.context.memMgr.Print()
				newtree.context.dbMgr.LogAllKeys()
				t.FailNow()
			}
		}
	}
}

func TestDBGetSetRootKey(t *testing.T) {
	defer glog.Flush()
	ctx, numElems, nlevels, numnodes := setupParams(common.MemMgrPolicyLocalLRU, common.DBMgrPolicyLocalMap)
	glog.Infof("enabling testpoint %v with freq of %d", testPointFailDBFetch, nlevels-1)
	TestPointEnable(testPointFailDBFetch, nlevels-1)
	glog.Infof("enabling testpoint %v with freq of %d", testPointFailDBUpdate, numElems/3)
	TestPointEnable(testPointFailDBUpdate, numElems/3)

	_, elemsInserted, _ := initAndInsert(t, *ctx, numElems)
	glog.Infof("resetting testpoint %v", testPointFailDBFetch)

	// All elements are inserted now, let's reinitialize the tree with new
	// memory manager, but old db manager. This should ensure that we
	// can read all the keys back. With actual persistent db manager preserving
	// db manager isn't needed.
	// Overwriting the memory mananger.
	ctx.memMgr = NewLocalLRUMemMgr(numnodes/2, common.KeyType(common.OrderedStrType), common.MemMgrPolicyLocalLRU)
	newtree, err := initTree(t, *ctx)
	if err != nil {
		glog.Errorf("reusing db manager. did not expect failure %v", err)
		t.FailNow()
	}
	// Can't have test point enabled during search otherwise search will fail.
	TestPointResetAll()
	for _, elem := range elemsInserted {
		glog.Infof("searching for elem: %v", elem)
		_, serr := newtree.Search(SearchSpecifier{searchKey: elem.TKey,
			direction: Exact})
		if serr != nil {
			glog.Errorf("failed to find elem/key: %v", elem)
			glog.Flush()
			t.FailNow()
		}
	}
}

func TestInsertAndSearchLocalLRUAndADBWithFailures(t *testing.T) {
	defer glog.Flush()
	ctx, numElems, nlevels, _ := setupParams(common.MemMgrPolicyLocalLRU, common.DBMgrPolicyADB)
	glog.Infof("Inserting %d elems", numElems)
	glog.Infof("enabling testpoint %v with freq of %d", testPointFailDBFetch, nlevels-1)
	TestPointEnable(testPointFailDBFetch, nlevels-1)
	glog.Infof("enabling testpoint %v with freq of %d", testPointFailDBUpdate, numElems/3)
	TestPointEnable(testPointFailDBUpdate, numElems/3)

	newtree, elemsInserted, _ := initAndInsert(t, *ctx, numElems)
	glog.Infof("resetting testpoint %v", testPointFailDBFetch)
	// Can't have test point enabled during search otherwise search will fail.
	TestPointResetAll()
	for _, elem := range elemsInserted {
		glog.Infof("searching for elem: %v", elem)
		_, serr := newtree.Search(SearchSpecifier{searchKey: elem.TKey,
			direction: Exact})
		if serr != nil {
			glog.Errorf("failed to find elem/key: %v", elem)
			glog.Flush()
			t.FailNow()
		}
	}
	dbm := ctx.dbMgr.(*ArangoDBMgr)
	dbm.DeleteTable(dbm.bptroot.TreeName)
	dbm.DeleteTable(dbm.cName)
}

func TestADB(t *testing.T) {
	defer glog.Flush()
	dbm, err := NewArangoDBMgr("http://localhost:8529", "bptest", "root", "root",
		"bplustree-roots", "tree_1_root")
	if err != nil {
		glog.Errorf("failed to init db: %v", err)
		t.FailNow()
	}

	rk := common.Key{BPTkey: "bptree-1-rootkey"}
	base := &Base{rk, 3}

	dk1 := common.Key{BPTkey: "tn1"}
	te1 := makeTestingVal(1)
	tne1 := treeNodeElem{NodeKey: common.Key{BPTkey: ""}, DataKey: dk1, Data: te1}
	dk2 := common.Key{BPTkey: "tn2"}
	te2 := makeTestingVal(2)
	tne2 := treeNodeElem{NodeKey: common.Key{BPTkey: ""}, DataKey: dk2, Data: te2}
	dk3 := common.Key{BPTkey: "tn3"}
	te3 := makeTestingVal(3)
	tne3 := treeNodeElem{NodeKey: common.Key{BPTkey: "bptree-1-nodekey-1"}, DataKey: dk3, Data: te3}

	node := &treeNode{}
	node.Children = make([]treeNodeElem, 3, 4)
	node.IsLeaf = true
	node.NextKey = common.Key{BPTkey: ""}
	node.PrevKey = common.Key{BPTkey: ""}
	// Generate a new key for the node being added.
	node.NodeKey = common.Key{BPTkey: "bptree-1-nodekey-1"}
	node.Children[0] = tne1
	node.Children[1] = tne2
	node.Children[2] = tne3

	dbUpdates := make([]common.DBOp, 2)
	dbUpdates[0] = common.DBOp{Op: common.DBOpStore, K: node.NodeKey, E: node}
	dbUpdates[1] = common.DBOp{Op: common.DBOpSetRoot, K: common.Key{BPTkey: ""}, E: base}
	err = dbm.AtomicUpdate(dbUpdates)
	if err != nil {
		glog.Errorf("failed to update.. %v", err)
		t.FailNow()
	}

	newb, err := dbm.GetRoot()
	if newb.Degree != base.Degree || newb.RootKey.Compare(base.RootKey) != 0 {
		glog.Errorf("failed.. ")
		t.FailNow()
	}
	val, err := dbm.Load(node.NodeKey)
	if err != nil || val == nil {
		glog.Errorf("failed to load %v from db (err: %v)", node.NodeKey, err)
		t.FailNow()
	}
	node1 := val.(*treeNode)

	if node.IsLeaf != node1.IsLeaf ||
		node.DataKey.Compare(node1.DataKey) != 0 ||
		node.NextKey.Compare(node1.NextKey) != 0 ||
		node.NodeKey.Compare(node1.NodeKey) != 0 ||
		node.PrevKey.Compare(node1.PrevKey) != 0 ||
		len(node.Children) != len(node1.Children) {
		glog.Errorf("values don't match")
		glog.Errorf("inserted: %v", node)
		glog.Errorf("fetched: %v", node1)
		t.FailNow()
	}

	for i := 0; i < len(node.Children); i++ {
		tne := node.Children[i]
		tne1 := node1.Children[i]
		tnev := getTestingVal(node.Children[i].Data)
		tnev1 := getTestingVal(node1.Children[i].Data)
		glog.Infof("value at %d: (expected: %v, got: %v)", i, tnev, tnev1)
		if tne.NodeKey.Compare(tne1.NodeKey) != 0 ||
			tne.DataKey.Compare(tne1.DataKey) != 0 ||
			tnev != tnev1 {
			glog.Errorf("elem values don't match at :%d", i)
			glog.Errorf("inserted: %v", tne)
			glog.Errorf("fetched: %v", tne1)
			t.FailNow()
		}
	}

	glog.Infof("%v", node)
	glog.Infof("%v", node1)

	// Doing the same update again.. it should take the update route and values
	// should still match.
	err = dbm.AtomicUpdate(dbUpdates)
	if err != nil {
		glog.Errorf("failed to update.. %v", err)
		t.FailNow()
	}
	newb, err = dbm.GetRoot()
	if newb.Degree != base.Degree || newb.RootKey.Compare(base.RootKey) != 0 {
		glog.Errorf("failed.. ")
		t.FailNow()
	}
	val, err = dbm.Load(node.NodeKey)
	if err != nil || val == nil {
		glog.Errorf("failed to load %v from db (err: %v)", node.NodeKey, err)
		t.FailNow()
	}
	node1 = val.(*treeNode)

	if node.IsLeaf != node1.IsLeaf ||
		node.DataKey.Compare(node1.DataKey) != 0 ||
		node.NextKey.Compare(node1.NextKey) != 0 ||
		node.NodeKey.Compare(node1.NodeKey) != 0 ||
		node.PrevKey.Compare(node1.PrevKey) != 0 ||
		len(node.Children) != len(node1.Children) {
		glog.Errorf("values don't match")
		glog.Errorf("inserted: %v", node)
		glog.Errorf("fetched: %v", node1)
		t.FailNow()
	}

	for i := 0; i < len(node.Children); i++ {
		tne := node.Children[i]
		tne1 := node1.Children[i]
		tnev := getTestingVal(node.Children[i].Data)
		tnev1 := getTestingVal(node1.Children[i].Data)
		glog.Infof("value at %d: (expected: %v, got: %v)", i, tnev, tnev1)
		if tne.NodeKey.Compare(tne1.NodeKey) != 0 ||
			tne.DataKey.Compare(tne1.DataKey) != 0 ||
			tnev != tnev1 {
			glog.Errorf("elem values don't match at :%d", i)
			glog.Errorf("inserted: %v", tne)
			glog.Errorf("fetched: %v", tne1)
			t.FailNow()
		}
	}

	// Try update and delete of a non-existent key.. this should fail the txn
	// and no updates should be made.
	oldKeyNode := node.DataKey
	oldKeyRoot := base.RootKey
	node.DataKey = common.Key{BPTkey: "bptree-1-nodekey-2"} // Updating node 1's key.
	base.RootKey = common.Key{BPTkey: "bptree-1-nodekey-2"}
	dbUpdates = make([]common.DBOp, 3)
	dbUpdates[0] = common.DBOp{Op: common.DBOpStore, K: node.NodeKey, E: node}
	dbUpdates[1] = common.DBOp{Op: common.DBOpSetRoot, K: common.Key{BPTkey: ""}, E: base}
	dbUpdates[1] = common.DBOp{Op: common.DBOpDelete, K: common.Key{BPTkey: "unknown"}, E: base}
	err = dbm.AtomicUpdate(dbUpdates)
	if err == nil {
		glog.Errorf("update succeeded.. was not supposed to")
		t.FailNow()
	}

	node.DataKey = oldKeyNode
	base.RootKey = oldKeyRoot

	// Nothing should have changed, let's verify against the old data.
	newb, err = dbm.GetRoot()
	if newb.Degree != base.Degree || newb.RootKey.Compare(base.RootKey) != 0 {
		glog.Errorf("failed.. ")
		t.FailNow()
	}
	val, err = dbm.Load(node.NodeKey)
	if err != nil || val == nil {
		glog.Errorf("failed to load %v from db (err: %v)", node.NodeKey, err)
		t.FailNow()
	}
	node1 = val.(*treeNode)

	if node.IsLeaf != node1.IsLeaf ||
		node.DataKey.Compare(node1.DataKey) != 0 ||
		node.NextKey.Compare(node1.NextKey) != 0 ||
		node.NodeKey.Compare(node1.NodeKey) != 0 ||
		node.PrevKey.Compare(node1.PrevKey) != 0 ||
		len(node.Children) != len(node1.Children) {
		glog.Errorf("values don't match")
		glog.Errorf("inserted: %v", node)
		glog.Errorf("fetched: %v", node1)
		t.FailNow()
	}

	for i := 0; i < len(node.Children); i++ {
		tne := node.Children[i]
		tne1 := node1.Children[i]
		tnev := getTestingVal(node.Children[i].Data)
		tnev1 := getTestingVal(node1.Children[i].Data)
		glog.Infof("value at %d: (expected: %v, got: %v)", i, tnev, tnev1)
		if tne.NodeKey.Compare(tne1.NodeKey) != 0 ||
			tne.DataKey.Compare(tne1.DataKey) != 0 ||
			tnev != tnev1 {
			glog.Errorf("elem values don't match at :%d", i)
			glog.Errorf("inserted: %v", tne)
			glog.Errorf("fetched: %v", tne1)
			t.FailNow()
		}
	}

	// New DB manager, using the old values.. let's ensure that the data is
	// intact.
	dbm1, err := NewArangoDBMgr("http://localhost:8529", "bptest", "root", "root",
		"bplustree-roots", "tree_1_root")
	if err != nil {
		glog.Errorf("failed to init db: %v", err)
		t.FailNow()
	}
	newb, err = dbm1.GetRoot()
	if newb.Degree != base.Degree || newb.RootKey.Compare(base.RootKey) != 0 {
		glog.Errorf("failed.. ")
		t.FailNow()
	}
	val, err = dbm1.Load(node.NodeKey)
	if err != nil || val == nil {
		glog.Errorf("failed to load %v from db (err: %v)", node.NodeKey, err)
		t.FailNow()
	}
	node1 = val.(*treeNode)

	if node.IsLeaf != node1.IsLeaf ||
		node.DataKey.Compare(node1.DataKey) != 0 ||
		node.NextKey.Compare(node1.NextKey) != 0 ||
		node.NodeKey.Compare(node1.NodeKey) != 0 ||
		node.PrevKey.Compare(node1.PrevKey) != 0 ||
		len(node.Children) != len(node1.Children) {
		glog.Errorf("values don't match")
		glog.Errorf("inserted: %v", node)
		glog.Errorf("fetched: %v", node1)
		t.FailNow()
	}

	for i := 0; i < len(node.Children); i++ {
		tne := node.Children[i]
		tne1 := node1.Children[i]
		tnev := getTestingVal(node.Children[i].Data)
		tnev1 := getTestingVal(node1.Children[i].Data)
		glog.Infof("value at %d: (expected: %v, got: %v)", i, tnev, tnev1)
		if tne.NodeKey.Compare(tne1.NodeKey) != 0 ||
			tne.DataKey.Compare(tne1.DataKey) != 0 ||
			tnev != tnev1 {
			glog.Errorf("elem values don't match at :%d", i)
			glog.Errorf("inserted: %v", tne)
			glog.Errorf("fetched: %v", tne1)
			t.FailNow()
		}
	}

	dbm1.DeleteTable(dbm1.bptroot.TreeName)
	dbm1.DeleteTable(dbm1.cName)
}
