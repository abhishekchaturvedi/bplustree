package bplustree

import (
	"bplustree/common"
	"context"
	"fmt"
	"reflect"
	"time"

	driver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/golang/glog"
	"github.com/mitchellh/mapstructure"
)

// ArangoDBDoc a document definition for arango DB that we use internally.
type ArangoDBDoc struct {
	Key   string      `json:"_key"`
	Value interface{} `json:"value"`
}

// This object stores the root node of the tree. The treeName points to the
// collection in database which contains the keys of the tree (treenodes)
// This collection is stored in the same db as the rootKey
type rootNode struct {
	TreeRoot Base   `json:"treeroot"`
	TreeName string `json:"treename"`
}

// ArangoDBMgr - object to represent the Arango DB persistence manager
// to be used as the DB manager with BplusTree.
// endpoint -- name of the endpoint to connect to (where DB can be reached)
// dbName   -- name of the database to use/create.
// db       -- db object
// dbClient -- db client.
// cName    -- collection name to be used for keys.
// rootKey  -- path/key (key where root of the bplus tree will be stored)
// bptroot  -- bplus tree base object (contains the key and the degree)
type ArangoDBMgr struct {
	endpoint string // this contains one or more endpoint URLs to connect
	dbName   string // database name for this driver
	db       driver.Database
	dbClient driver.Client
	cName    string
	rootKey  string
	bptroot  rootNode
}

// NewArangoDBMgr creates an object of type ArangoDBMgr.
// endpoint  - database endpoint
// username  - username to use to connect to db.
// passwd    - password to use to connect to db.
// cName     - name of the collection to check or create.
// rootkey   - root object key to check or create.
//
// This will connect to the db, create table (if it doesn't exist),
// create the collection to be used for the bplustree nodes (if it doesn't exist)
// In case of error, it returns nil object with error.
func NewArangoDBMgr(endpoint string, dbName, userName, passwd string,
	cName string, rootKey string) (*ArangoDBMgr, error) {

	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{endpoint},
		//TLSConfig: &tls.Config{InsecureSkipVerify: true},
	})
	if err != nil {
		glog.Errorf("could not connect to endpoints %s :: %v", endpoint, err)
		return nil, err
	}
	auth := driver.BasicAuthentication(userName, passwd)
	conn.SetAuthentication(auth)

	client, err := driver.NewClient(driver.ClientConfig{
		Connection: conn,
	})
	if err != nil {
		glog.Errorf("could not get a client for endpoints %s :: %v", endpoint, err)
		return nil, err
	}

	// check if the DB exists, if not found, create it
	ctx := context.Background()
	found, errDB := client.DatabaseExists(ctx, dbName)
	if errDB != nil {
		glog.Errorf("could not check existence of database %s :: %v", dbName, errDB)
		return nil, errDB
	}
	if !found {
		_, errC := client.CreateDatabase(ctx, dbName, nil)
		if errC != nil {
			glog.Errorf("could not create database %s :: %v", dbName, errC)
			return nil, errC
		}
	}
	// open the database that is specified in the dbName string
	db, err := client.Database(ctx, dbName)
	if err != nil {
		glog.Errorf("could not open up the database %s :: %v", dbName, err)
		return nil, err
	}
	glog.Infof("using database %s", db.Name())

	dbMgr := &ArangoDBMgr{
		endpoint: endpoint,
		dbName:   dbName,
		db:       db,
		dbClient: client,
	}

	err = dbMgr.CreateTable(cName)
	if err != nil {
		glog.Errorf("failed to create collection %v (err: %v)", cName, err)
		return nil, err
	}

	dbMgr.cName = cName
	dbMgr.rootKey = rootKey

	var bptroot rootNode
	val, rev, err := dbMgr.get(rootKey, cName)
	if err == common.ErrNotFound {
		// No root key found, which means that the tree doesn't exist. Create
		// a name for the treeNodes collection for this tree and create a table
		// to store the tree nodes.

		tn := fmt.Sprintf("%s_%s_%s_treeNodes_", dbName, cName, rootKey)
		bptroot = rootNode{TreeName: common.MakeRandString(tn, 16)}
		glog.Infof("No key exists yet. Genearating a random tree name (%v) to use for tree nodes collection",
			bptroot.TreeName)
		err = dbMgr.CreateTable(bptroot.TreeName)
		if err != nil {
			glog.Errorf("failed to create the tree nodes collection: %v (err: %v)",
				bptroot.TreeName, err)
			return nil, err
		}
		dbMgr.bptroot = bptroot
	} else if err != nil {
		glog.Errorf("failed to read root of tree (err: %v)", err)
		return nil, err
	} else {
		errDecode := Decode(val, &dbMgr.bptroot)
		if errDecode != nil {
			return nil, errDecode
		}
		glog.Infof("found existing root at: %v/%v with value: %v (rev: %v)", cName, rootKey, bptroot, rev)
	}

	return dbMgr, nil
}

// CreateTable creates a table or a collection with a given name.
func (mgr *ArangoDBMgr) CreateTable(name string) error {
	ctx := context.Background()
	cols, err := mgr.db.Collections(ctx)
	for ii := 0; ii < len(cols); ii++ {
		glog.Infof("collection = %s, database = %s", cols[ii].Name(), cols[ii].Database().Name())
	}
	// check if collection already exists
	found, err := mgr.db.CollectionExists(ctx, name)
	if err != nil {
		glog.Errorf("table exist check failed for %s :: %v", name, err)
		return err
	}
	if found {
		glog.Infof("collection %s exists, nothing else needed", name)
		return nil
	}
	options := &driver.CreateCollectionOptions{
		WaitForSync:       true,
		ReplicationFactor: 1, // TODO(ajay): make this to be cluster size
		NumberOfShards:    1, // TODO(ajay): make this higher based on cluster size
	}
	col, err := mgr.db.CreateCollection(ctx, name, options)
	if err != nil {
		glog.Errorf("could not create table %s :: %v", name, err)
		return err
	}
	glog.Infof("created table %s successfully", col.Name())
	return nil
}

// DeleteTable deletes a table or a collection with a given name.
func (mgr *ArangoDBMgr) DeleteTable(name string) error {
	ctx := context.Background()
	col, err := mgr.db.Collection(ctx, name)
	if err != nil {
		if driver.IsNotFound(err) {
			glog.Infof("collection not found : %s", name)
			return common.ErrNotFound
			//return util.NotFoundError(fmt.Sprintf("collection %s not found", name))
		}
		glog.Errorf("could not get collection %s :: %v", name, err)
		return err
	}
	errRem := col.Remove(ctx)
	if errRem != nil {
		glog.Errorf("could not delete collection %s :: %v", name, errRem)
		return errRem
	}
	return nil
}

// Get value of a given key.
func (mgr *ArangoDBMgr) get(key string, table string) (interface{}, string, error) {
	var doc ArangoDBDoc
	ctx := context.Background()
	col, err := mgr.db.Collection(ctx, table)
	if err != nil {
		glog.Errorf("could not get collection %s :: %v", table, err)
		return "", "", err
	}
	meta, err := col.ReadDocument(ctx, key, &doc)
	if err != nil {
		if driver.IsNotFound(err) {
			glog.Infof("key not found : %s", key)
			return "", "", common.ErrNotFound
			/* return "", "", util.NotFoundError(fmt.Sprintf("key %s not found", key)) */
		}
		glog.Errorf("could not read key %s :: %v", key, err)
		return "", "", err
	}
	return doc.Value, meta.Rev, nil
}

// put a value at a given key.
func (mgr *ArangoDBMgr) put(key string, table string, value interface{}) error {
	ctx := context.Background()
	col, err := mgr.db.Collection(ctx, table)
	if err != nil {
		glog.Errorf("could not get collection %s :: %v", table, err)
		return err
	}
	found, err := col.DocumentExists(ctx, key)
	if err != nil {
		glog.Errorf("could not check existence of key %s :: %v", key, err)
		return err
	}
	opctx := driver.WithWaitForSync(ctx)
	// create a new value with a _key field as key and use that
	newVal := ArangoDBDoc{
		Key:   key,
		Value: value,
	}
	if !found {
		_, err := col.CreateDocument(opctx, newVal)
		if err != nil {
			glog.Errorf("could not create key %s :: %v", key, err)
			return err
		}
	} else {
		_, err := col.UpdateDocument(opctx, key, newVal)
		if err != nil {
			glog.Errorf("could not update key %s :: %v", key, err)
			return err
		}
	}
	return nil
}

// delete the given key from the given table.
func (mgr *ArangoDBMgr) delete(key string, table string) error {
	ctx := context.Background()
	col, err := mgr.db.Collection(ctx, table)
	if err != nil {
		glog.Errorf("could not get collection %s :: %v", table, err)
		return err
	}
	ctxWithWait := driver.WithWaitForSync(ctx)
	_, errDel := col.RemoveDocument(ctxWithWait, key)
	if errDel != nil {
		if driver.IsNotFound(err) {
			// TODO(ajay): any special handling, right now we ignore this and return success
			glog.Errorf("could not remove key %s  as it was already not present:: %v", key, errDel)
			return nil
		}
		glog.Errorf("could not remove key %s :: %v", key, errDel)
		return errDel
	}
	return nil
}

// GetRoot - Get root base for the tree.
func (mgr *ArangoDBMgr) GetRoot() (*Base, error) {
	val, _, err := mgr.get(mgr.rootKey, mgr.cName)
	if err == common.ErrNotFound {
		glog.Errorf("failed to get the root key as it doesn't exist. perhaps a new tree is being created")
		// See interface definition.
		return nil, nil
	}
	if err != nil {
		glog.Errorf("failed to get root key: %v (err: %v)", mgr.rootKey, err)
		return nil, err
	}
	var bptroot rootNode
	glog.Infof("got root: %v", val)
	errDecode := Decode(val, &bptroot)
	if errDecode != nil {
		glog.Errorf("failed to decode root key: %v (err: %v)", mgr.rootKey, errDecode)
		return nil, errDecode
	}
	mgr.bptroot = bptroot
	return &mgr.bptroot.TreeRoot, nil
}

// SetRoot - Set root base for the tree.
func (mgr *ArangoDBMgr) SetRoot(base *Base) error {
	mgr.bptroot.TreeRoot = *base
	err := mgr.put(mgr.rootKey, mgr.cName, &mgr.bptroot)
	glog.V(1).Infof("storing root %v in db (val: %v) (err: %v)", mgr.rootKey,
		mgr.bptroot.TreeRoot, err)
	return err
}

// Store - store a key in the DB
func (mgr *ArangoDBMgr) Store(k common.Key, e interface{}) error {
	err := mgr.put(k.ToString(), mgr.bptroot.TreeName, e)
	glog.V(1).Infof("storing %v in db (val: %v) (err: %v)", k, e, err)
	return err
}

// Delete - deletes a key from the db
func (mgr *ArangoDBMgr) Delete(k common.Key) error {
	err := mgr.delete(k.ToString(), mgr.bptroot.TreeName)
	glog.V(1).Infof("deleting %v from db (err: %v)", k, err)
	return err
}

// AtomicUpdate - Updates the DB atomically with the provided ops.
func (mgr *ArangoDBMgr) AtomicUpdate(ops []common.DBOp) error {
	colOptions := driver.TransactionCollections{Exclusive: []string{mgr.cName,
		mgr.bptroot.TreeName}}
	ctx := context.Background()
	db := mgr.db
	colRoot, err := db.Collection(ctx, mgr.cName)
	if err != nil {
		glog.Errorf("failed to connect to table %v (err: %v)", mgr.cName, err)
		return err
	}
	colTreeNodes, err := db.Collection(ctx, mgr.bptroot.TreeName)
	if err != nil {
		glog.Errorf("failed to connect to table %v (err: %v)", mgr.bptroot.TreeName, err)
		return err
	}
	txid, err := db.BeginTransaction(ctx, colOptions, nil)
	if err != nil {
		glog.Errorf("failed to initiate the txn (err: %v)", err)
		return err
	}
	tctx := driver.WithTransactionID(ctx, txid)

	for i := 0; i < len(ops); i++ {
		switch {
		case ops[i].Op == common.DBOpStore:
			found, err := colTreeNodes.DocumentExists(ctx, ops[i].K.ToString())
			if err != nil {
				glog.Errorf("could not check existence of key %s :: %v", ops[i].K.ToString(), err)
				return err
			}
			newVal := ArangoDBDoc{
				Key:   ops[i].K.ToString(),
				Value: ops[i].E,
			}

			if !found {
				_, err = colTreeNodes.CreateDocument(tctx, newVal)
			} else {
				_, err = colTreeNodes.ReplaceDocument(tctx, ops[i].K.ToString(), newVal)
			}
		case ops[i].Op == common.DBOpDelete:
			_, err = colTreeNodes.RemoveDocument(tctx, ops[i].K.ToString())
		case ops[i].Op == common.DBOpSetRoot:
			found, err := colRoot.DocumentExists(ctx, mgr.rootKey)
			if err != nil {
				glog.Errorf("could not check existence of key %s :: %v", mgr.rootKey, err)
				return err
			}
			newRootNode := rootNode{TreeRoot: *(ops[i].E.(*Base)), TreeName: mgr.bptroot.TreeName}
			newVal := ArangoDBDoc{
				Key:   mgr.rootKey,
				Value: newRootNode,
			}

			if !found {
				_, err = colRoot.CreateDocument(tctx, newVal)
			} else {
				_, err = colRoot.ReplaceDocument(tctx, mgr.rootKey, newVal)
			}
		}
		if err != nil {
			glog.Errorf("failed to execute %v with key:%v (err: %v)", ops[i].Op, ops[i].K, err)
			break
		}
	}
	if err == nil {
		glog.Infof("Commiting transaction containing: %v", ops)
		err = db.CommitTransaction(ctx, txid, nil)
		if err != nil {
			glog.Errorf("failed to commit transaction (err: %v)", err)
			return err
		}
	} else {
		glog.Infof("Aborting transaction containing: %v", ops)
		errAbort := db.AbortTransaction(ctx, txid, nil)
		if errAbort != nil {
			glog.Errorf("failed to abort transaction (err: %v)", errAbort)
			return errAbort
		}
	}

	return err
}

// Load - Load a key from the DB
func (mgr *ArangoDBMgr) Load(k common.Key) (interface{}, error) {
	val, _, err := mgr.get(k.ToString(), mgr.bptroot.TreeName)
	if err != nil {
		glog.V(2).Infof("failed to load %v from db. err: %v", k, err)
		return nil, err
	}
	glog.V(2).Infof("loading %v from db. val: %v", k, val)
	var tn treeNode
	decodeErr := Decode(val, &tn)
	if decodeErr != nil {
		glog.Errorf("failed to decode treenode for %v (err: %v)", k, decodeErr)
		return nil, decodeErr
	}
	return &tn, nil
}

// LogAllKeys -- Prints the content of memory manager
func (mgr *ArangoDBMgr) LogAllKeys() {
	glog.Infof("Printing all keys")
}

// Policy -- Get the policy name for this manager.
func (mgr *ArangoDBMgr) Policy() string {
	return common.DBMgrPolicyADB
}

// TimeHookFunc handles decoding of a time format field
func TimeHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if t != reflect.TypeOf(time.Time{}) {
			return data, nil
		}

		switch f.Kind() {
		case reflect.String:
			return time.Parse(time.RFC3339, data.(string))
		case reflect.Float64:
			return time.Unix(0, int64(data.(float64))*int64(time.Millisecond)), nil
		case reflect.Int64:
			return time.Unix(0, data.(int64)*int64(time.Millisecond)), nil
		default:
			return data, nil
		}
		// Convert it by parsing
	}
}

// Decode converts a map to a struct by mapping key names to
// fields in the struct. It uses a mapstructure library to do the task.
func Decode(input interface{}, result interface{}) error {
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Metadata: nil,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			TimeHookFunc()),
		Result: result,
	})
	if err != nil {
		return err
	}

	if err := decoder.Decode(input); err != nil {
		return err
	}
	return err
}
