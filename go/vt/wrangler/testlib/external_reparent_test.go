/*
Copyright 2017 Google Inc.

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
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// The tests in this package test the wrangler version of TabletExternallyReparented
// This is the one that is now called by the vtctl command

// TestTabletExternallyReparentedBasic tests the base cases for TER
func TestTabletExternallyReparentedBasic(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create an old main, a new main, two good replicas, one bad replica
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, oldMain.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// On the elected main, we will respond to
	// TabletActionSubordinateWasPromoted
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// On the old main, we will only respond to
	// TabletActionSubordinateWasRestarted.
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)

	// First test: reparent to the same main, make sure it works
	// as expected.
	if err := vp.Run([]string{"TabletExternallyReparented", topoproto.TabletAliasString(oldMain.Tablet.Alias)}); err != nil {
		t.Fatalf("TabletExternallyReparented(same main) should have worked: %v", err)
	}

	// check the old main is still main
	tablet, err := ts.GetTablet(ctx, oldMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("old main should be MASTER but is: %v", tablet.Type)
	}

	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new main) expecting success")
	if err := wr.TabletExternallyReparented(ctx, newMain.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}

	// check the new main is main
	tablet, err = ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new main should be MASTER but is: %v", tablet.Type)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldMain.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
			}
			t.Fatalf("old main (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMain.Tablet.Alias), tablet.Type)
		}
		// check the old main was converted to replica
		tablet, err = ts.GetTablet(ctx, oldMain.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_REPLICA {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}
}

func TestTabletExternallyReparentedToSubordinate(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old main, a new main, two good replicas, one bad replica
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	newMain.FakeMysqlDaemon.ReadOnly = true
	newMain.FakeMysqlDaemon.Replicating = true

	// Build keyspace graph
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, oldMain.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// On the elected main, we will respond to
	// TabletActionSubordinateWasPromoted
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// On the old main, we will only respond to
	// TabletActionSubordinateWasRestarted.
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)

	// Second test: reparent to a replica, and pretend the old
	// main is still good to go.
	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}

	// This tests a bad case: the new designated main is a subordinate at mysql level,
	// but we should do what we're told anyway.
	if err := wr.TabletExternallyReparented(ctx, newMain.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) error: %v", err)
	}

	// check that newMain is main
	tablet, err := ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new main should be MASTER but is: %v", tablet.Type)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldMain.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
			}
			t.Fatalf("old main (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMain.Tablet.Alias), tablet.Type)
		}
		// check the old main was converted to replica
		tablet, err = ts.GetTablet(ctx, oldMain.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_REPLICA {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}
}

// TestTabletExternallyReparentedWithDifferentMysqlPort makes sure
// that if mysql is restarted on the main-elect tablet and has a different
// port, we pick it up correctly.
func TestTabletExternallyReparentedWithDifferentMysqlPort(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old main, a new main, two good replicas, one bad replica
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

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

	// On the good replicas, we will respond to
	// TabletActionSubordinateWasRestarted and point to the new mysql port
	goodReplica.StartActionLoop(t, wr)
	defer goodReplica.StopActionLoop(t)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new main) expecting success")
	if err := wr.TabletExternallyReparented(ctx, newMain.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	// check the new main is main
	tablet, err := ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new main should be MASTER but is: %v", tablet.Type)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldMain.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
			}
			t.Fatalf("old main (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMain.Tablet.Alias), tablet.Type)
		}
		// check the old main was converted to replica
		tablet, err = ts.GetTablet(ctx, oldMain.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_REPLICA {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}
}

// TestTabletExternallyReparentedContinueOnUnexpectedMain makes sure
// that we ignore mysql's main if the flag is set
func TestTabletExternallyReparentedContinueOnUnexpectedMain(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old main, a new main, two good replicas, one bad replica
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

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

	// On the good replica, we will respond to
	// TabletActionSubordinateWasRestarted and point to a bad host
	goodReplica.StartActionLoop(t, wr)
	defer goodReplica.StopActionLoop(t)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new main) expecting success")
	if err := wr.TabletExternallyReparented(ctx, newMain.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	// check the new main is main
	tablet, err := ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new main should be MASTER but is: %v", tablet.Type)
	}
	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldMain.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
			}
			t.Fatalf("old main (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMain.Tablet.Alias), tablet.Type)
		}
		// check the old main was converted to replica
		tablet, err = ts.GetTablet(ctx, oldMain.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_REPLICA {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}
}

func TestTabletExternallyReparentedRerun(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old main, a new main, and a good replica.
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

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

	goodReplica.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	// On the good replica, we will respond to
	// TabletActionSubordinateWasRestarted.
	goodReplica.StartActionLoop(t, wr)
	defer goodReplica.StopActionLoop(t)

	// The reparent should work as expected here
	if err := wr.TabletExternallyReparented(ctx, newMain.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}

	// check the new main is main
	tablet, err := ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new main should be MASTER but is: %v", tablet.Type)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldMain.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
			}
			t.Fatalf("old main (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMain.Tablet.Alias), tablet.Type)
		}
		// check the old main was converted to replica
		tablet, err = ts.GetTablet(ctx, oldMain.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_REPLICA {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}

	// run TER again and make sure the main is still correct
	if err := wr.TabletExternallyReparented(ctx, newMain.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}

	// check the new main is still main
	tablet, err = ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMain.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new main should be MASTER but is: %v", tablet.Type)
	}

}

func TestRPCTabletExternallyReparentedDemotesMainToConfiguredTabletType(t *testing.T) {
	flag.Set("demote_main_type", "spare")
	flag.Set("disable_active_reparents", "true")

	// Reset back to default values
	defer func() {
		flag.Set("demote_main_type", "replica")
		flag.Set("disable_active_reparents", "false")
	}()

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old main and a new main
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_SPARE, nil)

	oldMain.StartActionLoop(t, wr)
	newMain.StartActionLoop(t, wr)

	defer oldMain.StopActionLoop(t)
	defer newMain.StopActionLoop(t)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMain.Tablet.Keyspace, []string{"cell1"})
	assert.NoError(t, err, "RebuildKeyspaceLocked failed: %v", err)

	// Reparent to new main
	ti, err := ts.GetTablet(ctx, newMain.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}

	if err := wr.TabletExternallyReparented(context.Background(), ti.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented failed: %v", err)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err := ts.GetTablet(ctx, oldMain.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
			}
			t.Fatalf("old main (%v) should be spare but is: %v", topoproto.TabletAliasString(oldMain.Tablet.Alias), tablet.Type)
		}
		// check the old main was converted to replica
		tablet, err := ts.GetTablet(ctx, oldMain.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldMain.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_SPARE {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}

	shardInfo, err := ts.GetShard(context.Background(), newMain.Tablet.Keyspace, newMain.Tablet.Shard)
	assert.NoError(t, err)

	assert.True(t, topoproto.TabletAliasEqual(newMain.Tablet.Alias, shardInfo.MainAlias))
	assert.Equal(t, topodatapb.TabletType_MASTER, newMain.Agent.Tablet().Type)
}
