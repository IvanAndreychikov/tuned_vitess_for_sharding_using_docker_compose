/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testlib

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestRPCTabletExternallyReparented(t *testing.T) {
	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old main and a new main
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell2", 1, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMain.Tablet.Keyspace, []string{"cell1", "cell2"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// On the elected main, we will respond to
	// TabletActionSubordinateWasPromoted
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	// On the old main, we will only respond to
	// TabletActionSubordinateWasRestarted.
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)

	// First test: reparent to the same main, make sure it works
	// as expected.
	tmc := tmclient.NewTabletManagerClient()
	_, err = ts.GetTablet(ctx, oldMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), oldMain.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(same main) should have worked: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparented: same main", waitID)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new main) expecting success")
	ti, err := ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID = makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparented: good case", waitID)
}

func TestRPCTabletExternallyReparentedSubordinateMysql(t *testing.T) {
	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old main, a new main, two good subordinates, one bad subordinate
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell2", 1, topodatapb.TabletType_REPLICA, nil)
	newMain.FakeMysqlDaemon.ReadOnly = true
	newMain.FakeMysqlDaemon.Replicating = true

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMain.Tablet.Keyspace, []string{"cell1", "cell2"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// On the elected main, we will respond to
	// TabletActionSubordinateWasPromoted
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	// On the old main, we will only respond to
	// TabletActionSubordinateWasRestarted.
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)

	// This tests a bad case: the new designated main is a subordinate,
	// but we should do what we're told anyway.
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(subordinate) error: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparented: subordinate designated as main", waitID)

}

// TestRPCTabletExternallyReparentedWithDifferentMysqlPort makes sure
// that if mysql is restarted on the main-elect tablet and has a different
// port, we pick it up correctly.
func TestRPCTabletExternallyReparentedWithDifferentMysqlPort(t *testing.T) {
	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old main, a new main, two good subordinates, one bad subordinate
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSubordinate := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMain.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// Now we're restarting mysql on a different port, 3301->3303
	// but without updating the Tablet record in topology.

	// On the elected main, we will respond to
	// TabletActionSubordinateWasPromoted, so we need a MysqlDaemon
	// that returns no main, and the new port (as returned by mysql)
	newMain.FakeMysqlDaemon.MysqlPort = 3303
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	// On the old main, we will only respond to
	// TabletActionSubordinateWasRestarted and point to the new mysql port
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)

	// On the good subordinates, we will respond to
	// TabletActionSubordinateWasRestarted and point to the new mysql port
	goodSubordinate.StartActionLoop(t, wr)
	defer goodSubordinate.StopActionLoop(t)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new main) expecting success")
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedWithDifferentMysqlPort: good case", waitID)
}

// TestRPCTabletExternallyReparentedContinueOnUnexpectedMain makes sure
// that we ignore mysql's main if the flag is set
func TestRPCTabletExternallyReparentedContinueOnUnexpectedMain(t *testing.T) {
	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old main, a new main, two good subordinates, one bad subordinate
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSubordinate := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMain.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// On the elected main, we will respond to
	// TabletActionSubordinateWasPromoted, so we need a MysqlDaemon
	// that returns no main, and the new port (as returned by mysql)
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	// On the old main, we will only respond to
	// TabletActionSubordinateWasRestarted and point to a bad host
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)

	// On the good subordinate, we will respond to
	// TabletActionSubordinateWasRestarted and point to a bad host
	goodSubordinate.StartActionLoop(t, wr)
	defer goodSubordinate.StopActionLoop(t)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new main) expecting success")
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedContinueOnUnexpectedMain: good case", waitID)
}

func TestRPCTabletExternallyReparentedFailedOldMain(t *testing.T) {
	// The 'RefreshState' call on the old main will timeout on
	// this value, so it has to be smaller than the 10s of the
	// wait for the 'finished' state of waitForExternalReparent.
	tabletmanager.SetReparentFlags(2 * time.Second /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old main, a new main, and a good subordinate.
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSubordinate := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMain.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// Reparent to a replica, and pretend the old main is not responding.

	// On the elected main, we will respond to
	// TabletActionSubordinateWasPromoted.
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// On the old main, we will only get a RefreshState call,
	// let's just not respond to it at all, and let it timeout.
	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}

	// On the good subordinate, we will respond to
	// TabletActionSubordinateWasRestarted.
	goodSubordinate.StartActionLoop(t, wr)
	defer goodSubordinate.StopActionLoop(t)

	// The reparent should work as expected here
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedFailedOldMain: good case", waitID)

	// check the old main was converted to replica
	tablet, err := ts.GetTablet(ctx, oldMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_REPLICA {
		t.Fatalf("old main should be replica but is: %v", tablet.Type)
	}
}

func TestRPCTabletExternallyReparentedImpostorMain(t *testing.T) {
	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old main, a new main, and a bad subordinate.
	badSubordinate := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_MASTER, nil)
	// do this after badSubordinate so that the shard record has the expected main
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil, ForceInitTablet())
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMain.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// check the old main is really main
	tablet, err := ts.GetTablet(ctx, oldMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("old main should be MASTER but is: %v", tablet.Type)
	}

	// check the impostor also claims to be main
	tablet, err = ts.GetTablet(ctx, badSubordinate.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", badSubordinate.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("impostor should be MASTER but is: %v", tablet.Type)
	}

	// On the elected main, we will respond to
	// TabletActionSubordinateWasPromoted.
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	// On the old main, we will only respond to
	// TabletActionSubordinateWasRestarted.
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)

	// set this to old main because as soon as badSubordinate starts, it detects that
	// there is a main with a later timestamp and demotes itself
	badSubordinate.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(oldMain.Tablet)
	badSubordinate.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	// On the bad subordinate, we will respond to
	// TabletActionSubordinateWasRestarted.
	badSubordinate.StartActionLoop(t, wr)
	defer badSubordinate.StopActionLoop(t)

	// The reparent should work as expected here
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedImpostorMain: good case", waitID)

	// check the new main is really main
	tablet, err = ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new main should be MASTER but is: %v", tablet.Type)
	}

	// check the old main was converted to replica
	tablet, err = ts.GetTablet(ctx, oldMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_REPLICA {
		t.Fatalf("old main should be replica but is: %v", tablet.Type)
	}

	// check the impostor main was converted to replica
	tablet, err = ts.GetTablet(ctx, badSubordinate.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", badSubordinate.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_REPLICA {
		t.Fatalf("bad subordinate should be replica but is: %v", tablet.Type)
	}
}

func TestRPCTabletExternallyReparentedFailedImpostorMain(t *testing.T) {
	tabletmanager.SetReparentFlags(2 * time.Second /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old main, a new main, and a bad subordinate.
	badSubordinate := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_MASTER, nil)
	// do this after badSubordinate so that the shard record has the expected main
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil, ForceInitTablet())
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMain.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// check the old main is really main
	tablet, err := ts.GetTablet(ctx, oldMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("old main should be MASTER but is: %v", tablet.Type)
	}

	// check the impostor also claims to be main
	tablet, err = ts.GetTablet(ctx, badSubordinate.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", badSubordinate.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("old main should be MASTER but is: %v", tablet.Type)
	}

	// On the elected main, we will respond to
	// TabletActionSubordinateWasPromoted.
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	// On the old main, we will only respond to
	// TabletActionSubordinateWasRestarted.
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)

	// The reparent should work as expected here
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedImpostorMain: good case", waitID)

	// check the new main is really main
	tablet, err = ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new main should be MASTER but is: %v", tablet.Type)
	}

	// check the old main was converted to replica
	tablet, err = ts.GetTablet(ctx, oldMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_REPLICA {
		t.Fatalf("old main should be replica but is: %v", tablet.Type)
	}

	// check the impostor main was converted to replica
	tablet, err = ts.GetTablet(ctx, badSubordinate.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", badSubordinate.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_REPLICA {
		t.Fatalf("bad subordinate should be replica but is: %v", tablet.Type)
	}
}

func TestRPCTabletExternallyReparentedRerun(t *testing.T) {
	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old main, a new main, and a good subordinate.
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSubordinate := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMain.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// On the elected main, we will respond to
	// TabletActionSubordinateWasPromoted.
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	// On the old main, we will only respond to
	// TabletActionSubordinateWasRestarted.
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)

	// On the good subordinate, we will respond to
	// TabletActionSubordinateWasRestarted.
	goodSubordinate.StartActionLoop(t, wr)
	defer goodSubordinate.StopActionLoop(t)

	// The reparent should work as expected here
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedFailedOldMain: good case", waitID)

	// check the old main was converted to replica
	tablet, err := ts.GetTablet(ctx, oldMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_REPLICA {
		t.Fatalf("old main should be replica but is: %v", tablet.Type)
	}

	// run TER again and make sure the main is still correct
	waitID = makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedFailedOldMain: good case", waitID)

	// check the new main is still main
	tablet, err = ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new main should be MASTER but is: %v", tablet.Type)
	}

}

var (
	externalReparents      = make(map[string]chan struct{})
	externalReparentsMutex sync.Mutex
)

// makeWaitID generates a unique externalID that can be passed to
// TabletExternallyReparented, and then to waitForExternalReparent.
func makeWaitID() string {
	externalReparentsMutex.Lock()
	id := fmt.Sprintf("wait id %v", len(externalReparents))
	externalReparents[id] = make(chan struct{})
	externalReparentsMutex.Unlock()
	return id
}

func init() {
	event.AddListener(func(ev *events.Reparent) {
		if ev.Status == "finished" {
			externalReparentsMutex.Lock()
			if c, ok := externalReparents[ev.ExternalID]; ok {
				close(c)
			}
			externalReparentsMutex.Unlock()
		}
	})
}

// waitForExternalReparent waits up to a fixed duration for the external
// reparent with the given ID to finish. The ID must have been previously
// generated by makeWaitID().
//
// The TabletExternallyReparented RPC returns as soon as the
// new main is visible in the serving graph. Before checking things like
// replica endpoints and old main status, we should wait for the finalize
// stage, which happens in the background.
func waitForExternalReparent(t *testing.T, name, externalID string) {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	externalReparentsMutex.Lock()
	c := externalReparents[externalID]
	externalReparentsMutex.Unlock()

	select {
	case <-c:
		return
	case <-timer.C:
		t.Fatalf("deadline exceeded waiting for finalized external reparent %q for test %v", externalID, name)
	}
}
