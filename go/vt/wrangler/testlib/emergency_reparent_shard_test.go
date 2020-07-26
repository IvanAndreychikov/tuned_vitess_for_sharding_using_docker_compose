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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestEmergencyReparentShard(t *testing.T) {
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
	newMain.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	newMain.FakeMysqlDaemon.PromoteResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// old main, will be scrapped
	oldMain.FakeMysqlDaemon.ReadOnly = false
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)

	// good replica 1 is replicating
	goodReplica1.FakeMysqlDaemon.ReadOnly = true
	goodReplica1.FakeMysqlDaemon.Replicating = true
	goodReplica1.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 455,
			},
		},
	}
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
	goodReplica2.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 454,
			},
		},
	}
	goodReplica2.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodReplica2.StartActionLoop(t, wr)
	goodReplica2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodReplica2.StopActionLoop(t)

	// run EmergencyReparentShard
	err := vp.Run([]string{"EmergencyReparentShard", "-wait_subordinate_timeout", "10s", newMain.Tablet.Keyspace + "/" + newMain.Tablet.Shard,
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
	// old main read-only flag doesn't matter, it is scrapped
	assert.True(t, goodReplica1.FakeMysqlDaemon.ReadOnly, "goodReplica1.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, goodReplica2.FakeMysqlDaemon.ReadOnly, "goodReplica2.FakeMysqlDaemon.ReadOnly not set")
	assert.True(t, goodReplica1.FakeMysqlDaemon.Replicating, "goodReplica1.FakeMysqlDaemon.Replicating not set")
	assert.False(t, goodReplica2.FakeMysqlDaemon.Replicating, "goodReplica2.FakeMysqlDaemon.Replicating set")
	checkSemiSyncEnabled(t, true, true, newMain)
	checkSemiSyncEnabled(t, false, true, goodReplica1, goodReplica2)
}

// TestEmergencyReparentShardMainElectNotBest tries to emergency reparent
// to a host that is not the latest in replication position.
func TestEmergencyReparentShardMainElectNotBest(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create a main, a couple good replicas
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	moreAdvancedReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// new main
	newMain.FakeMysqlDaemon.Replicating = true
	// this server has executed upto 455, which is the highest among replicas
	newMain.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 455,
			},
		},
	}
	// It has more transactions in its relay log, but not as many as
	// moreAdvancedReplica
	newMain.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
	}
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// old main, will be scrapped
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)

	// more advanced replica
	moreAdvancedReplica.FakeMysqlDaemon.Replicating = true
	// position up to which this replica has executed is behind desired new main
	moreAdvancedReplica.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 454,
			},
		},
	}
	// relay log position is more advanced than desired new main
	moreAdvancedReplica.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}
	moreAdvancedReplica.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
	}
	moreAdvancedReplica.StartActionLoop(t, wr)
	defer moreAdvancedReplica.StopActionLoop(t)

	// run EmergencyReparentShard
	err := wr.EmergencyReparentShard(ctx, newMain.Tablet.Keyspace, newMain.Tablet.Shard, newMain.Tablet.Alias, 10*time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "is more advanced than main elect tablet")
	// check what was run
	err = newMain.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = oldMain.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
	err = moreAdvancedReplica.FakeMysqlDaemon.CheckSuperQueryList()
	require.NoError(t, err)
}
