package bplustree

import (
	"github.com/golang/glog"
)

// TestPointID -- data type defining a testpoint identifier.
type TestPointID int

const (
	testPointInvalid      TestPointID = 0
	testPointFailDBFetch              = 1
	testPointFailDBUpdate             = 2
	testPointMax                      = 100
)

// TestPoint -- describes a given test point.
// tpID    - ID of the test point
// desc    - string description of the test point.
// enabled - whether this test point is enabled.
// iters   - # of iterations this test point is executed.
// freq    - frequency at which this test point will be executed.
type TestPoint struct {
	tpID    TestPointID
	desc    string
	enabled bool
	iters   int
	freq    int
}

var testPoints = [...]TestPoint{
	{tpID: testPointInvalid, desc: "max", enabled: false, iters: 0, freq: 0},
	{tpID: testPointFailDBFetch, desc: "fail DB fetch", enabled: false, iters: 0, freq: 0},
	{tpID: testPointFailDBUpdate, desc: "fail DB update", enabled: false, iters: 0, freq: 0},
	{tpID: testPointMax, desc: "max", enabled: false, iters: 0, freq: 0},
}

var numTestPointsEnabled int = 0

func isValidTestPoint(tpID TestPointID) bool {
	if tpID > testPointInvalid && tpID < testPointMax {
		return true
	}
	return false
}

// TestPointIsEnabled - Check to see if a given test point is enabled. If the
// test point specified is 'invalid' or 'max' then returns true or false based
// on if any of the test points are enabled.
func TestPointIsEnabled(tpID TestPointID) (bool, int) {
	enabled := false
	freq := 0
	glog.V(2).Infof("%v", testPoints)
	if isValidTestPoint(tpID) {
		enabled = testPoints[tpID].enabled
		freq = testPoints[tpID].freq
	} else {
		if numTestPointsEnabled > 0 {
			enabled = true
		} else {
			enabled = false
		}
	}
	return enabled, freq
}

// TestPointResetAll - reset all test points.
func TestPointResetAll() {
	glog.Infof("disabling all test points")
	for _, tp := range testPoints {
		if isValidTestPoint(tp.tpID) {
			TestPointReset(tp.tpID)
		}
	}
}

// TestPointEnable - enable a given test point to be executed at given freq.
// if already enabled, this call will be a no-op
// If frequency is 0 or negative, this is a no-op.
// otherwise testpoint is enabled to be executed every 'freq' interval.
func TestPointEnable(tpID TestPointID, freq int) {
	glog.V(2).Infof("%v", testPoints)
	if freq > 0 && isValidTestPoint(tpID) && !testPoints[tpID].enabled {
		numTestPointsEnabled++
		testPoints[tpID].enabled = true
		testPoints[tpID].freq = freq
		glog.Infof("enabling test point %v", testPoints[tpID])
	} else if isValidTestPoint(tpID) {
		glog.V(2).Infof("testpoint already enabled? : %v (%d)", testPoints[tpID], freq)
	} else {
		glog.V(2).Infof("Ignoring enable call for %v, %d", tpID, freq)
	}
}

// TestPointDisable - disables a test point temporarily.
// if already disabled, this call will be a no-op. Note that it doesn't reset
// any settings for the test point and just disables it.
func TestPointDisable(tpID TestPointID) {
	if isValidTestPoint(tpID) && testPoints[tpID].enabled {
		testPoints[tpID].enabled = false
		glog.Infof("disabling test point %v", testPoints[tpID])
	}
}

// TestPointReset - Reset a given test point so that it will not be executed.
// if the tpID is invalid, then all test points are reset.
func TestPointReset(tpID TestPointID) {
	if !isValidTestPoint(tpID) {
		return
	}
	numTestPointsEnabled--
	testPoints[tpID].enabled = false
	testPoints[tpID].freq = 0
	testPoints[tpID].iters = 0
	glog.Infof("reseting test point %v", testPoints[tpID])
}

// TestPointExecute - Execute a given testpoint if it's enabled and the settings
// indicate that it should.
// returns true if the checkpoint should execute.
func TestPointExecute(tpID TestPointID) bool {
	if !isValidTestPoint(tpID) {
		glog.V(2).Infof("Invalid test point %v", tpID)
		return false
	}

	if testPoints[tpID].enabled {
		testPoints[tpID].iters++
		if (testPoints[tpID].iters % testPoints[tpID].freq) == 0 {
			glog.Infof("executing testpoint %s (iter: %d, freq: %d)",
				testPoints[tpID].desc, testPoints[tpID].iters, testPoints[tpID].freq)
			return true
		}
		glog.V(2).Infof("testpoint %v not executed", testPoints[tpID])
	} else {
		glog.V(2).Infof("testpoint %v is disabled", testPoints[tpID])
	}
	return false
}
