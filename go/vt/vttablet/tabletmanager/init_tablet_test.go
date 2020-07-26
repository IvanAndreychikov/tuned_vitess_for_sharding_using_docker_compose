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

package tabletmanager

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/history"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Init tablet fixes replication data when safe
func TestInitTabletFixesReplicationData(t *testing.T) {
	ctx := context.Background()
	cell := "cell1"
	ts := memorytopo.NewServer(cell, "cell2")
	tabletAlias := &topodatapb.TabletAlias{
		Cell: cell,
		Uid:  1,
	}

	// start with a tablet record that doesn't exist
	port := int32(1234)
	gRPCPort := int32(3456)
	mysqlDaemon := fakemysqldaemon.NewFakeMysqlDaemon(nil)
	agent := &ActionAgent{
		TopoServer:  ts,
		TabletAlias: tabletAlias,
		MysqlDaemon: mysqlDaemon,
		DBConfigs:   &dbconfigs.DBConfigs{},
		batchCtx:    ctx,
		History:     history.New(historyLength),
		_healthy:    fmt.Errorf("healthcheck not run yet"),
	}

	// 1. Initialize the tablet as REPLICA.
	*tabletHostname = "localhost"
	*initKeyspace = "test_keyspace"
	*initShard = "-C0"
	*initTabletType = "replica"
	tabletAlias = &topodatapb.TabletAlias{
		Cell: cell,
		Uid:  2,
	}
	agent.TabletAlias = tabletAlias
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("InitTablet(type) failed: %v", err)
	}
	sri, err := ts.GetShardReplication(ctx, cell, *initKeyspace, "-c0")
	if err != nil || len(sri.Nodes) != 1 || !proto.Equal(sri.Nodes[0].TabletAlias, tabletAlias) {
		t.Fatalf("Created ShardReplication doesn't match: %v %v", sri, err)
	}

	// Remove the ShardReplication record, try to create the
	// tablets again, make sure it's fixed.
	if err := topo.RemoveShardReplicationRecord(ctx, ts, cell, *initKeyspace, "-c0", tabletAlias); err != nil {
		t.Fatalf("RemoveShardReplicationRecord failed: %v", err)
	}
	sri, err = ts.GetShardReplication(ctx, cell, *initKeyspace, "-c0")
	if err != nil || len(sri.Nodes) != 0 {
		t.Fatalf("Modifed ShardReplication doesn't match: %v %v", sri, err)
	}

	// Initialize the same tablet again, CreateTablet will fail, but it should recreate shard replication data
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("InitTablet(type) failed: %v", err)
	}

	sri, err = ts.GetShardReplication(ctx, cell, *initKeyspace, "-c0")
	if err != nil || len(sri.Nodes) != 1 || !proto.Equal(sri.Nodes[0].TabletAlias, tabletAlias) {
		t.Fatalf("Created ShardReplication doesn't match: %v %v", sri, err)
	}
}

// This is a test to make sure a regression does not happen in the future.
// There is code in InitTablet that updates replication data if tablet fails
// to be created due to a NodeExists error. During this particular error we were not doing
// the sanity checks that the provided tablet was the same in the topo.
func TestInitTabletDoesNotUpdateReplicationDataForTabletInWrongShard(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	tabletAlias := &topodatapb.TabletAlias{
		Cell: "cell1",
		Uid:  1,
	}

	// start with a tablet record that doesn't exist
	port := int32(1234)
	gRPCPort := int32(3456)
	mysqlDaemon := fakemysqldaemon.NewFakeMysqlDaemon(nil)
	agent := &ActionAgent{
		TopoServer:  ts,
		TabletAlias: tabletAlias,
		MysqlDaemon: mysqlDaemon,
		DBConfigs:   &dbconfigs.DBConfigs{},
		batchCtx:    ctx,
		History:     history.New(historyLength),
		_healthy:    fmt.Errorf("healthcheck not run yet"),
	}

	// 1. Initialize the tablet as REPLICA.
	*tabletHostname = "localhost"
	*initKeyspace = "test_keyspace"
	*initShard = "-C0"
	*initTabletType = "replica"
	tabletAlias = &topodatapb.TabletAlias{
		Cell: "cell1",
		Uid:  2,
	}
	agent.TabletAlias = tabletAlias
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("InitTablet(type) failed: %v", err)
	}
	tabletAliases, err := ts.FindAllTabletAliasesInShard(ctx, "test_keyspace", "-c0")
	if err != nil {
		t.Fatalf("Could not fetch tablet aliases for shard: %v", err)
	}

	if len(tabletAliases) != 1 {
		t.Fatalf("Expected to have only one tablet alias, got: %v", len(tabletAliases))
	}
	if tabletAliases[0].Uid != 2 {
		t.Fatalf("Expected table UID be equal to 2, got: %v", tabletAliases[0].Uid)
	}

	// Try to initialize a tablet with the same uid in a different shard.
	*initShard = "-D0"
	if err := agent.InitTablet(port, gRPCPort); err == nil {
		t.Fatalf("InitTablet(type) should have failed, got nil")
	}

	if tablets, _ := ts.FindAllTabletAliasesInShard(ctx, "test_keyspace", "-d0"); len(tablets) != 0 {
		t.Fatalf("Tablet shouldn't be added to replication data")
	}
}

// TestInitTablet will test the InitTablet code creates / updates the
// tablet node correctly. Note we modify global parameters (the flags)
// so this has to be in one test.
func TestInitTablet(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	tabletAlias := &topodatapb.TabletAlias{
		Cell: "cell1",
		Uid:  1,
	}
	db := fakesqldb.New(t)
	defer db.Close()

	// start with a tablet record that doesn't exist
	port := int32(1234)
	gRPCPort := int32(3456)
	mysqlDaemon := fakemysqldaemon.NewFakeMysqlDaemon(db)
	agent := &ActionAgent{
		TopoServer:  ts,
		TabletAlias: tabletAlias,
		MysqlDaemon: mysqlDaemon,
		DBConfigs:   &dbconfigs.DBConfigs{},
		VREngine:    vreplication.NewTestEngine(nil, "", nil, nil, "", nil),
		batchCtx:    ctx,
		History:     history.New(historyLength),
		_healthy:    fmt.Errorf("healthcheck not run yet"),
	}

	// 1. Initialize the tablet as REPLICA.
	// This will create the respective topology records.
	// We use a capitalized shard name here, to make sure the
	// Keyrange computation works, fills in the KeyRange, and converts
	// it to lower case.
	*tabletHostname = "localhost"
	*initKeyspace = "test_keyspace"
	*initShard = "-C0"
	*initTabletType = "replica"
	tabletAlias = &topodatapb.TabletAlias{
		Cell: "cell1",
		Uid:  2,
	}

	_, err := agent.TopoServer.GetSrvKeyspace(ctx, "cell1", "test_keyspace")
	switch {
	case topo.IsErrType(err, topo.NoNode):
		// srvKeyspace should not be when tablets haven't been registered to this cell
	default:
		t.Fatalf("GetSrvKeyspace failed: %v", err)
	}

	agent.TabletAlias = tabletAlias
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("InitTablet(type) failed: %v", err)
	}
	si, err := ts.GetShard(ctx, "test_keyspace", "-c0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	_, err = agent.TopoServer.GetSrvKeyspace(ctx, "cell1", "test_keyspace")
	switch {
	case err != nil:
		// srvKeyspace should not be when tablets haven't been registered to this cell
	default:
		t.Errorf("Serving keyspace was not generated for cell: %v", si)
	}

	ti, err := ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}
	if ti.Hostname != "localhost" {
		t.Errorf("wrong hostname for tablet: %v", ti.Hostname)
	}
	if ti.PortMap["vt"] != port {
		t.Errorf("wrong port for tablet: %v", ti.PortMap["vt"])
	}
	if ti.PortMap["grpc"] != gRPCPort {
		t.Errorf("wrong gRPC port for tablet: %v", ti.PortMap["grpc"])
	}
	if ti.Shard != "-c0" {
		t.Errorf("wrong shard for tablet: %v", ti.Shard)
	}
	if string(ti.KeyRange.Start) != "" || string(ti.KeyRange.End) != "\xc0" {
		t.Errorf("wrong KeyRange for tablet: %v", ti.KeyRange)
	}
	if got := agent._mainTermStartTime; !got.IsZero() {
		t.Fatalf("REPLICA tablet should not have a MainTermStartTime set: %v", got)
	}

	// 2. Update shard's main to our alias, then try to init again.
	// (This simulates the case where the MainAlias in the shard record says
	// that we are the main but the tablet record says otherwise. In that case,
	// we assume we are not the MASTER.)
	_, err = agent.TopoServer.UpdateShardFields(ctx, "test_keyspace", "-c0", func(si *topo.ShardInfo) error {
		si.MainAlias = tabletAlias
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("InitTablet(type, healthcheck) failed: %v", err)
	}
	ti, err = ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	// It should still be replica, because the tablet record doesn't agree.
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}
	if got := agent._mainTermStartTime; !got.IsZero() {
		t.Fatalf("REPLICA tablet should not have a mainTermStartTime set: %v", got)
	}

	// 3. Delete the tablet record. The shard record still says that we are the
	// MASTER. Since it is the only source, we assume that its information is
	// correct and start as MASTER.
	if err := ts.DeleteTablet(ctx, tabletAlias); err != nil {
		t.Fatalf("DeleteTablet failed: %v", err)
	}
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("InitTablet(type, healthcheck) failed: %v", err)
	}
	ti, err = ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_MASTER {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}
	ter1 := agent._mainTermStartTime
	if ter1.IsZero() {
		t.Fatalf("MASTER tablet should have a mainTermStartTime set")
	}

	// 4. Fix the tablet record to agree that we're main.
	// Shard and tablet record are in sync now and we assume that we are actually
	// the MASTER.
	ti.Type = topodatapb.TabletType_MASTER
	if err := ts.UpdateTablet(ctx, ti); err != nil {
		t.Fatalf("UpdateTablet failed: %v", err)
	}
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("InitTablet(type, healthcheck) failed: %v", err)
	}
	ti, err = ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_MASTER {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}
	ter2 := agent._mainTermStartTime
	if ter2.IsZero() || !ter2.Equal(ter1) {
		t.Fatalf("After a restart, mainTermStartTime must be equal to the previous time saved in the tablet record. Previous timestamp: %v current timestamp: %v", ter1, ter2)
	}

	// 5. Subsequent inits will still start the vttablet as MASTER.
	// (Also check db name override and tags here.)
	*initDbNameOverride = "DBNAME"
	initTags.Set("aaa:bbb")
	if err := agent.InitTablet(port, gRPCPort); err != nil {
		t.Fatalf("InitTablet(type, healthcheck) failed: %v", err)
	}
	ti, err = ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_MASTER {
		t.Errorf("wrong tablet type: %v", ti.Type)
	}
	if ti.DbNameOverride != "DBNAME" {
		t.Errorf("wrong tablet DbNameOverride: %v", ti.DbNameOverride)
	}
	if len(ti.Tags) != 1 || ti.Tags["aaa"] != "bbb" {
		t.Errorf("wrong tablet tags: %v", ti.Tags)
	}
	ter3 := agent._mainTermStartTime
	if ter3.IsZero() || !ter3.Equal(ter2) {
		t.Fatalf("After a restart, mainTermStartTime must be set to the previous time saved in the tablet record. Previous timestamp: %v current timestamp: %v", ter2, ter3)
	}
}
