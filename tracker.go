package bplustree

import (
	"bplustree/common"

	"github.com/golang/glog"
)

// Tracker -- book keeping map for a key.
type Tracker struct {
	kmap     map[common.Key]interface{}
	tag      string
	deepcopy bool
}

// AddIfNotPresent -- checks for existence and adds the element into the map if
// not already exists.
func (m *Tracker) AddIfNotPresent(k common.Key, e interface{}) {
	glog.V(2).Infof("Checking for %v (%v) in %s", k, e, m.tag)
	_, ok := m.kmap[k]
	if !ok {
		if m.deepcopy {
			tn := e.(*treeNode)
			m.kmap[k] = tn.deepCopy()
		} else {
			m.kmap[k] = e
		}
		glog.V(2).Infof("Tracking %v (%v) in %s", k, m.kmap[k], m.tag)
	}
}

// Init -- initialize the map.
func (m *Tracker) Init(tag string, deepcopy bool) {
	m.kmap = make(map[common.Key]interface{})
	m.tag = tag
	m.deepcopy = deepcopy
}

// Reset -- reset the map
func (m *Tracker) Reset() {
	m.kmap = nil
	m.deepcopy = false
}
