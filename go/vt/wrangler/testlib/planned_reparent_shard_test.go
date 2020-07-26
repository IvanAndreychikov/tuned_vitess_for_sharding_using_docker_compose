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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestPlannedReparentShardNoMainProvided(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a main, a couple good replicas
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell2", 2, topodatapb.TabletType_REPLICA, nil)

	// new main
	newMain.FakeMysqlDaemon.ReadOnly = true
	newMain.FakeMysqlDaemon.Replicating = true
	newMain.FakeMysqlDaemon.WaitMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMain.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// old main
	oldMain.FakeMysqlDaemon.ReadOnly = false
	oldMain.FakeMysqlDaemon.Replicating = false
	oldMain.FakeMysqlDaemon.SubordinateStatusError = mysql.ErrNotSubordinate
	oldMain.FakeMysqlDaemon.CurrentMainPosition = newMain.FakeMysqlDaemon.WaitMainPosition
	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
		// we end up calling SetMain twice on the old main
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)
	oldMain.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// SetMain is called on new main to make sure it's replicating before reparenting.
	newMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(oldMain.Tablet)

	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", newMain.Tablet.Keyspace + "/" + newMain.Tablet.Shard})
	require.NoError(t, err)

	// check what was run
	err = newMain.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	err = oldMain.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	err = goodReplica1.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, newMain.FakeMysqlDaemon.ReadOnly, "newMain.FakeMysqlDaemon.ReadOnly is set")
	assert.True(t, oldMain.FakeMysqlDaemon.ReadOnly, "oldMain.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, oldMain.Agent.QueryServiceControl.IsServing(), "oldMain...QueryServiceControl not serving")

	// verify the old main was told to start replicating (and not
	// the replica that wasn't replicating in the first place)
	assert.True(t, oldMain.FakeMysqlDaemon.Replicating, "oldMain.FakeMysqlDaemon.Replicating not set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.Replicating, "goodReplica1.FakeMysqlDaemon.Replicating not set")
	checkSemiSyncEnabled(t, true, true, newMain)
	checkSemiSyncEnabled(t, false, true, goodReplica1, oldMain)
}

func TestPlannedReparentShardNoError(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a main, a couple good replicas
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// new main
	newMain.FakeMysqlDaemon.ReadOnly = true
	newMain.FakeMysqlDaemon.Replicating = true
	newMain.FakeMysqlDaemon.WaitMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMain.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// old main
	oldMain.FakeMysqlDaemon.ReadOnly = false
	oldMain.FakeMysqlDaemon.Replicating = false
	oldMain.FakeMysqlDaemon.SubordinateStatusError = mysql.ErrNotSubordinate
	oldMain.FakeMysqlDaemon.CurrentMainPosition = newMain.FakeMysqlDaemon.WaitMainPosition
	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
		// we end up calling SetMain twice on the old main
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)
	oldMain.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// SetMain is called on new main to make sure it's replicating before reparenting.
	newMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(oldMain.Tablet)

	// goodReplica1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// goodReplica2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.Replicating = false
	goodReplica2.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", newMain.Tablet.Keyspace + "/" + newMain.Tablet.Shard, "-new_main",
		topoproto.TabletAliasString(newMain.Tablet.Alias)})
	require.NoError(t, err)

	// check what was run
	err = newMain.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = oldMain.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica1.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica2.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, newMain.FakeMysqlDaemon.ReadOnly, "newMain.FakeMysqlDaemon.ReadOnly set")
	assert.True(t, oldMain.FakeMysqlDaemon.ReadOnly, "oldMain.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")

	assert.True(t, goodReplica2.FakeMysqlDaemon.ReadOnly, "goodReplica2.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, oldMain.Agent.QueryServiceControl.IsServing(), "oldMain...QueryServiceControl not serving")

	// verify the old main was told to start replicating (and not
	// the replica that wasn't replicating in the first place)
	assert.True(t, oldMain.FakeMysqlDaemon.Replicating, "oldMain.FakeMysqlDaemon.Replicating not set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.Replicating, "goodReplica1.FakeMysqlDaemon.Replicating not set")
	assert.False(t, goodReplica2.FakeMysqlDaemon.Replicating, "goodReplica2.FakeMysqlDaemon.Replicating set")

	checkSemiSyncEnabled(t, true, true, newMain)
	checkSemiSyncEnabled(t, false, true, goodReplica1, goodReplica2, oldMain)
}

func TestPlannedReparentNoMain(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a few replicas.
	replica1 := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_REPLICA, nil)
	NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", replica1.Tablet.Keyspace + "/" + replica1.Tablet.Shard, "-new_main", topoproto.TabletAliasString(replica1.Tablet.Alias)})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "the shard has no main")
}

// TestPlannedReparentShardWaitForPositionFail simulates a failure of the WaitForPosition call
// on the desired new main tablet
func TestPlannedReparentShardWaitForPositionFail(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a main, a couple good replicas
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// new main
	newMain.FakeMysqlDaemon.ReadOnly = true
	newMain.FakeMysqlDaemon.Replicating = true
	newMain.FakeMysqlDaemon.WaitMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMain.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// old main
	oldMain.FakeMysqlDaemon.ReadOnly = false
	oldMain.FakeMysqlDaemon.Replicating = false
	// set to incorrect value to make promote fail on WaitForMainPos
	oldMain.FakeMysqlDaemon.CurrentMainPosition = newMain.FakeMysqlDaemon.PromoteResult
	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)
	oldMain.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)
	// SetMain is called on new main to make sure it's replicating before reparenting.
	newMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(oldMain.Tablet)

	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// good replica 2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.Replicating = false
	goodReplica2.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", newMain.Tablet.Keyspace + "/" + newMain.Tablet.Shard, "-new_main", topoproto.TabletAliasString(newMain.Tablet.Alias)})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "replication on main-elect cell1-0000000001 did not catch up in time")

	// now check that DemoteMain was undone and old main is still main
	assert.True(t, newMain.FakeMysqlDaemon.ReadOnly, "newMain.FakeMysqlDaemon.ReadOnly not set")
	assert.False(t, oldMain.FakeMysqlDaemon.ReadOnly, "oldMain.FakeMysqlDaemon.ReadOnly set")
}

// TestPlannedReparentShardWaitForPositionTimeout simulates a context timeout
// during the WaitForPosition call to the desired new main
func TestPlannedReparentShardWaitForPositionTimeout(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a main, a couple good replicas
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// new main
	newMain.FakeMysqlDaemon.TimeoutHook = func() error { return context.DeadlineExceeded }
	newMain.FakeMysqlDaemon.ReadOnly = true
	newMain.FakeMysqlDaemon.Replicating = true
	newMain.FakeMysqlDaemon.WaitMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMain.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// old main
	oldMain.FakeMysqlDaemon.ReadOnly = false
	oldMain.FakeMysqlDaemon.Replicating = false
	oldMain.FakeMysqlDaemon.CurrentMainPosition = newMain.FakeMysqlDaemon.WaitMainPosition
	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)
	oldMain.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// SetMain is called on new main to make sure it's replicating before reparenting.
	newMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(oldMain.Tablet)
	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START replica",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// good replica 2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.Replicating = false
	goodReplica2.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", newMain.Tablet.Keyspace + "/" + newMain.Tablet.Shard, "-new_main", topoproto.TabletAliasString(newMain.Tablet.Alias)})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "replication on main-elect cell1-0000000001 did not catch up in time")

	// now check that DemoteMain was undone and old main is still main
	assert.True(t, newMain.FakeMysqlDaemon.ReadOnly, "newMain.FakeMysqlDaemon.ReadOnly not set")
	assert.False(t, oldMain.FakeMysqlDaemon.ReadOnly, "oldMain.FakeMysqlDaemon.ReadOnly set")
}

func TestPlannedReparentShardRelayLogError(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a main, a couple good replicas
	main := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// old main
	main.FakeMysqlDaemon.ReadOnly = false
	main.FakeMysqlDaemon.Replicating = false
	main.FakeMysqlDaemon.SubordinateStatusError = mysql.ErrNotSubordinate
	main.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	main.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	main.StartActionLoop(t, wr)
	defer main.StopActionLoop(t)
	main.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// goodReplica1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(main.Tablet)
	// simulate error that will trigger a call to RestartSubordinate
	goodReplica1.FakeMysqlDaemon.SetMainError = errors.New("Subordinate failed to initialize relay log info structure from the repository")
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", main.Tablet.Keyspace + "/" + main.Tablet.Shard, "-new_main",
		topoproto.TabletAliasString(main.Tablet.Alias)})
	require.NoError(t, err)
	// check what was run
	err = main.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica1.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, main.FakeMysqlDaemon.ReadOnly, "main.FakeMysqlDaemon.ReadOnly set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, main.Agent.QueryServiceControl.IsServing(), "main...QueryServiceControl not serving")

	// verify the old main was told to start replicating (and not
	// the replica that wasn't replicating in the first place)
	assert.True(t, goodReplica1.FakeMysqlDaemon.Replicating, "goodReplica1.FakeMysqlDaemon.Replicating not set")
}

func TestPlannedReparentShardRelayLogErrorStartSubordinate(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a main, a couple good replicas
	main := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// old main
	main.FakeMysqlDaemon.ReadOnly = false
	main.FakeMysqlDaemon.Replicating = false
	main.FakeMysqlDaemon.SubordinateStatusError = mysql.ErrNotSubordinate
	main.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	main.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	main.StartActionLoop(t, wr)
	defer main.StopActionLoop(t)
	main.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// good replica 1 is not replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SubordinateIORunning = false
	goodReplica1.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(main.Tablet)
	goodReplica1.FakeMysqlDaemon.CurrentMainHost = topoproto.MysqlHostname(main.Tablet)
	goodReplica1.FakeMysqlDaemon.CurrentMainPort = int(topoproto.MysqlPort(main.Tablet))
	// simulate error that will trigger a call to RestartSubordinate
	goodReplica1.FakeMysqlDaemon.StartSubordinateError = errors.New("Subordinate failed to initialize relay log info structure from the repository")
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", main.Tablet.Keyspace + "/" + main.Tablet.Shard, "-new_main",
		topoproto.TabletAliasString(main.Tablet.Alias)})
	require.NoError(t, err)
	// check what was run
	err = main.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = goodReplica1.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)

	assert.False(t, main.FakeMysqlDaemon.ReadOnly, "main.FakeMysqlDaemon.ReadOnly set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, main.Agent.QueryServiceControl.IsServing(), "main...QueryServiceControl not serving")

	// verify the old main was told to start replicating (and not
	// the replica that wasn't replicating in the first place)
	assert.True(t, goodReplica1.FakeMysqlDaemon.Replicating, "goodReplica1.FakeMysqlDaemon.Replicating not set")
}

// TestPlannedReparentShardPromoteReplicaFail simulates a failure of the PromoteReplica call
// on the desired new main tablet
func TestPlannedReparentShardPromoteReplicaFail(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a main, a couple good replicas
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// new main
	newMain.FakeMysqlDaemon.ReadOnly = true
	newMain.FakeMysqlDaemon.Replicating = true
	// make promote fail
	newMain.FakeMysqlDaemon.PromoteError = errors.New("some error")
	newMain.FakeMysqlDaemon.WaitMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMain.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// old main
	oldMain.FakeMysqlDaemon.ReadOnly = false
	oldMain.FakeMysqlDaemon.Replicating = false
	oldMain.FakeMysqlDaemon.SubordinateStatusError = mysql.ErrNotSubordinate
	oldMain.FakeMysqlDaemon.CurrentMainPosition = newMain.FakeMysqlDaemon.WaitMainPosition
	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)
	oldMain.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// SetMain is called on new main to make sure it's replicating before reparenting.
	newMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(oldMain.Tablet)
	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// good replica 2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.Replicating = false
	goodReplica2.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", newMain.Tablet.Keyspace + "/" + newMain.Tablet.Shard, "-new_main", topoproto.TabletAliasString(newMain.Tablet.Alias)})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "some error")

	// when promote fails, we don't call UndoDemoteMain, so the old main should be read-only
	assert.True(t, newMain.FakeMysqlDaemon.ReadOnly, "newMain.FakeMysqlDaemon.ReadOnly")
	assert.True(t, oldMain.FakeMysqlDaemon.ReadOnly, "oldMain.FakeMysqlDaemon.ReadOnly")

	// retrying should work
	newMain.FakeMysqlDaemon.PromoteError = nil
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		// extra commands because of retry
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
		// extra commands because of retry
		"FAKE SET MASTER",
		"START SLAVE",
	}

	// run PlannedReparentShard
	err = vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", newMain.Tablet.Keyspace + "/" + newMain.Tablet.Shard, "-new_main", topoproto.TabletAliasString(newMain.Tablet.Alias)})
	require.NoError(t, err)

	// check that mainship changed correctly
	assert.False(t, newMain.FakeMysqlDaemon.ReadOnly, "newMain.FakeMysqlDaemon.ReadOnly")
	assert.True(t, oldMain.FakeMysqlDaemon.ReadOnly, "oldMain.FakeMysqlDaemon.ReadOnly")
}

// TestPlannedReparentShardSameMain tests PRS with oldMain works correctly
// Simulate failure of previous PRS and oldMain is ReadOnly
// Verify that main correctly gets set to ReadWrite
func TestPlannedReparentShardSameMain(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a main, a couple good replicas
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	goodReplica1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodReplica2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// old main
	oldMain.FakeMysqlDaemon.ReadOnly = true
	oldMain.FakeMysqlDaemon.Replicating = false
	oldMain.FakeMysqlDaemon.SubordinateStatusError = mysql.ErrNotSubordinate
	oldMain.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			7: mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)
	oldMain.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(oldMain.Tablet)
	goodReplica1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica1.StartActionLoop(t, wr)
	defer goodReplica1.StopActionLoop(t)

	// goodReplica2 is not replicating
	goodReplica2.FakeMysqlDaemon.ReadOnly = true
	goodReplica2.FakeMysqlDaemon.Replicating = false
	goodReplica2.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(oldMain.Tablet)
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", oldMain.Tablet.Keyspace + "/" + oldMain.Tablet.Shard, "-new_main", topoproto.TabletAliasString(oldMain.Tablet.Alias)})
	require.NoError(t, err)
	assert.False(t, oldMain.FakeMysqlDaemon.ReadOnly, "oldMain.FakeMysqlDaemon.ReadOnly")
}
