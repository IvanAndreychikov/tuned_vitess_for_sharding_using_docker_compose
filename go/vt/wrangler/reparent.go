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

package wrangler

/*
This file handles the reparenting operations.
*/

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	initShardMainOperation            = "InitShardMain"
	plannedReparentShardOperation       = "PlannedReparentShard"
	emergencyReparentShardOperation     = "EmergencyReparentShard"
	tabletExternallyReparentedOperation = "TabletExternallyReparented"
)

// ShardReplicationStatuses returns the ReplicationStatus for each tablet in a shard.
func (wr *Wrangler) ShardReplicationStatuses(ctx context.Context, keyspace, shard string) ([]*topo.TabletInfo, []*replicationdatapb.Status, error) {
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return nil, nil, err
	}
	tablets := topotools.CopyMapValues(tabletMap, []*topo.TabletInfo{}).([]*topo.TabletInfo)

	wr.logger.Infof("Gathering tablet replication status for: %v", tablets)
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	result := make([]*replicationdatapb.Status, len(tablets))

	for i, ti := range tablets {
		// Don't scan tablets that won't return something
		// useful. Otherwise, you'll end up waiting for a timeout.
		if ti.Type == topodatapb.TabletType_MASTER {
			wg.Add(1)
			go func(i int, ti *topo.TabletInfo) {
				defer wg.Done()
				pos, err := wr.tmc.MainPosition(ctx, ti.Tablet)
				if err != nil {
					rec.RecordError(fmt.Errorf("MainPosition(%v) failed: %v", ti.AliasString(), err))
					return
				}
				result[i] = &replicationdatapb.Status{
					Position: pos,
				}
			}(i, ti)
		} else if ti.IsSubordinateType() {
			wg.Add(1)
			go func(i int, ti *topo.TabletInfo) {
				defer wg.Done()
				status, err := wr.tmc.SubordinateStatus(ctx, ti.Tablet)
				if err != nil {
					rec.RecordError(fmt.Errorf("SubordinateStatus(%v) failed: %v", ti.AliasString(), err))
					return
				}
				result[i] = status
			}(i, ti)
		}
	}
	wg.Wait()
	return tablets, result, rec.Error()
}

// ReparentTablet tells a tablet to reparent this tablet to the current
// main, based on the current replication position. If there is no
// match, it will fail.
func (wr *Wrangler) ReparentTablet(ctx context.Context, tabletAlias *topodatapb.TabletAlias) error {
	// Get specified tablet.
	// Get current shard main tablet.
	// Sanity check they are in the same keyspace/shard.
	// Issue a SetMain to the tablet.
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	shardInfo, err := wr.ts.GetShard(ctx, ti.Keyspace, ti.Shard)
	if err != nil {
		return err
	}
	if !shardInfo.HasMain() {
		return fmt.Errorf("no main tablet for shard %v/%v", ti.Keyspace, ti.Shard)
	}

	mainTi, err := wr.ts.GetTablet(ctx, shardInfo.MainAlias)
	if err != nil {
		return err
	}

	// Basic sanity checking.
	if mainTi.Type != topodatapb.TabletType_MASTER {
		return fmt.Errorf("TopologyServer has inconsistent state for shard main %v", topoproto.TabletAliasString(shardInfo.MainAlias))
	}
	if mainTi.Keyspace != ti.Keyspace || mainTi.Shard != ti.Shard {
		return fmt.Errorf("main %v and potential subordinate not in same keyspace/shard", topoproto.TabletAliasString(shardInfo.MainAlias))
	}

	// and do the remote command
	return wr.tmc.SetMain(ctx, ti.Tablet, shardInfo.MainAlias, 0, "", false)
}

// InitShardMain will make the provided tablet the main for the shard.
func (wr *Wrangler) InitShardMain(ctx context.Context, keyspace, shard string, mainElectTabletAlias *topodatapb.TabletAlias, force bool, waitReplicasTimeout time.Duration) (err error) {
	// lock the shard
	ctx, unlock, lockErr := wr.ts.LockShard(ctx, keyspace, shard, fmt.Sprintf("InitShardMain(%v)", topoproto.TabletAliasString(mainElectTabletAlias)))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// do the work
	err = wr.initShardMainLocked(ctx, ev, keyspace, shard, mainElectTabletAlias, force, waitReplicasTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed InitShardMain: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished InitShardMain")
	}
	return err
}

func (wr *Wrangler) initShardMainLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, mainElectTabletAlias *topodatapb.TabletAlias, force bool, waitReplicasTimeout time.Duration) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading tablet map")
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// Check the main elect is in tabletMap.
	mainElectTabletAliasStr := topoproto.TabletAliasString(mainElectTabletAlias)
	mainElectTabletInfo, ok := tabletMap[mainElectTabletAliasStr]
	if !ok {
		return fmt.Errorf("main-elect tablet %v is not in the shard", topoproto.TabletAliasString(mainElectTabletAlias))
	}
	ev.NewMain = *mainElectTabletInfo.Tablet

	// Check the main is the only main is the shard, or -force was used.
	_, mainTabletMap := topotools.SortedTabletMap(tabletMap)
	if !topoproto.TabletAliasEqual(shardInfo.MainAlias, mainElectTabletAlias) {
		if !force {
			return fmt.Errorf("main-elect tablet %v is not the shard main, use -force to proceed anyway", topoproto.TabletAliasString(mainElectTabletAlias))
		}
		wr.logger.Warningf("main-elect tablet %v is not the shard main, proceeding anyway as -force was used", topoproto.TabletAliasString(mainElectTabletAlias))
	}
	if _, ok := mainTabletMap[mainElectTabletAliasStr]; !ok {
		if !force {
			return fmt.Errorf("main-elect tablet %v is not a main in the shard, use -force to proceed anyway", topoproto.TabletAliasString(mainElectTabletAlias))
		}
		wr.logger.Warningf("main-elect tablet %v is not a main in the shard, proceeding anyway as -force was used", topoproto.TabletAliasString(mainElectTabletAlias))
	}
	haveOtherMain := false
	for alias := range mainTabletMap {
		if mainElectTabletAliasStr != alias {
			haveOtherMain = true
		}
	}
	if haveOtherMain {
		if !force {
			return fmt.Errorf("main-elect tablet %v is not the only main in the shard, use -force to proceed anyway", topoproto.TabletAliasString(mainElectTabletAlias))
		}
		wr.logger.Warningf("main-elect tablet %v is not the only main in the shard, proceeding anyway as -force was used", topoproto.TabletAliasString(mainElectTabletAlias))
	}

	// First phase: reset replication on all tablets. If anyone fails,
	// we stop. It is probably because it is unreachable, and may leave
	// an unstable database process in the mix, with a database daemon
	// at a wrong replication spot.

	// Create a context for the following RPCs that respects waitReplicasTimeout
	resetCtx, resetCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer resetCancel()

	event.DispatchUpdate(ev, "resetting replication on all tablets")
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for alias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(alias string, tabletInfo *topo.TabletInfo) {
			defer wg.Done()
			wr.logger.Infof("resetting replication on tablet %v", alias)
			if err := wr.tmc.ResetReplication(resetCtx, tabletInfo.Tablet); err != nil {
				rec.RecordError(fmt.Errorf("tablet %v ResetReplication failed (either fix it, or Scrap it): %v", alias, err))
			}
		}(alias, tabletInfo)
	}
	wg.Wait()
	if err := rec.Error(); err != nil {
		// if any of the subordinates failed
		return err
	}

	// Check we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Tell the new main to break its subordinates, return its replication
	// position
	wr.logger.Infof("initializing main on %v", topoproto.TabletAliasString(mainElectTabletAlias))
	event.DispatchUpdate(ev, "initializing main")
	rp, err := wr.tmc.InitMain(ctx, mainElectTabletInfo.Tablet)
	if err != nil {
		return err
	}

	// Check we stil have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Create a cancelable context for the following RPCs.
	// If error conditions happen, we can cancel all outgoing RPCs.
	replCtx, replCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer replCancel()

	// Now tell the new main to insert the reparent_journal row,
	// and tell everybody else to become a subordinate of the new main,
	// and wait for the row in the reparent_journal table.
	// We start all these in parallel, to handle the semi-sync
	// case: for the main to be able to commit its row in the
	// reparent_journal table, it needs connected subordinates.
	event.DispatchUpdate(ev, "reparenting all tablets")
	now := time.Now().UnixNano()
	wgMain := sync.WaitGroup{}
	wgSubordinates := sync.WaitGroup{}
	var mainErr error
	for alias, tabletInfo := range tabletMap {
		if alias == mainElectTabletAliasStr {
			wgMain.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgMain.Done()
				wr.logger.Infof("populating reparent journal on new main %v", alias)
				mainErr = wr.tmc.PopulateReparentJournal(replCtx, tabletInfo.Tablet, now, initShardMainOperation, mainElectTabletAlias, rp)
			}(alias, tabletInfo)
		} else {
			wgSubordinates.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgSubordinates.Done()
				wr.logger.Infof("initializing subordinate %v", alias)
				if err := wr.tmc.InitSubordinate(replCtx, tabletInfo.Tablet, mainElectTabletAlias, rp, now); err != nil {
					rec.RecordError(fmt.Errorf("tablet %v InitSubordinate failed: %v", alias, err))
				}
			}(alias, tabletInfo)
		}
	}

	// After the main is done, we can update the shard record
	// (note with semi-sync, it also means at least one subordinate is done).
	wgMain.Wait()
	if mainErr != nil {
		// The main failed, there is no way the
		// subordinates will work.  So we cancel them all.
		wr.logger.Warningf("main failed to PopulateReparentJournal, canceling subordinates")
		replCancel()
		wgSubordinates.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on main: %v", mainErr)
	}
	if !topoproto.TabletAliasEqual(shardInfo.MainAlias, mainElectTabletAlias) {
		if _, err := wr.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
			si.MainAlias = mainElectTabletAlias
			return nil
		}); err != nil {
			wgSubordinates.Wait()
			return fmt.Errorf("failed to update shard main record: %v", err)
		}
	}

	// Wait for the subordinates to complete. If some of them fail, we
	// don't want to rebuild the shard serving graph (the failure
	// will most likely be a timeout, and our context will be
	// expired, so the rebuild will fail anyway)
	wgSubordinates.Wait()
	if err := rec.Error(); err != nil {
		return err
	}

	// Create database if necessary on the main. Subordinates will get it too through
	// replication. Since the user called InitShardMain, they've told us to
	// assume that whatever data is on all the subordinates is what they intended.
	// If the database doesn't exist, it means the user intends for these tablets
	// to begin serving with no data (i.e. first time initialization).
	createDB := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sqlescape.EscapeID(topoproto.TabletDbName(mainElectTabletInfo.Tablet)))
	if _, err := wr.tmc.ExecuteFetchAsDba(ctx, mainElectTabletInfo.Tablet, false, []byte(createDB), 1, false, true); err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}

	return nil
}

// PlannedReparentShard will make the provided tablet the main for the shard,
// when both the current and new main are reachable and in good shape.
func (wr *Wrangler) PlannedReparentShard(ctx context.Context, keyspace, shard string, mainElectTabletAlias, avoidMainAlias *topodatapb.TabletAlias, waitReplicasTimeout time.Duration) (err error) {
	// lock the shard
	lockAction := fmt.Sprintf(
		"PlannedReparentShard(%v, avoid_main=%v)",
		topoproto.TabletAliasString(mainElectTabletAlias),
		topoproto.TabletAliasString(avoidMainAlias))
	ctx, unlock, lockErr := wr.ts.LockShard(ctx, keyspace, shard, lockAction)
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// Attempt to set avoidMainAlias if not provided by parameters
	if mainElectTabletAlias == nil && avoidMainAlias == nil {
		shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return err
		}
		avoidMainAlias = shardInfo.MainAlias
	}

	// do the work
	err = wr.plannedReparentShardLocked(ctx, ev, keyspace, shard, mainElectTabletAlias, avoidMainAlias, waitReplicasTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed PlannedReparentShard: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished PlannedReparentShard")
	}
	return err
}

func (wr *Wrangler) plannedReparentShardLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, mainElectTabletAlias, avoidMainTabletAlias *topodatapb.TabletAlias, waitReplicasTimeout time.Duration) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading tablet map")
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// Check invariants we're going to depend on.
	if topoproto.TabletAliasEqual(mainElectTabletAlias, avoidMainTabletAlias) {
		return fmt.Errorf("main-elect tablet %v is the same as the tablet to avoid", topoproto.TabletAliasString(mainElectTabletAlias))
	}
	if mainElectTabletAlias == nil {
		if !topoproto.TabletAliasEqual(avoidMainTabletAlias, shardInfo.MainAlias) {
			event.DispatchUpdate(ev, "current main is different than -avoid_main, nothing to do")
			return nil
		}
		event.DispatchUpdate(ev, "searching for main candidate")
		mainElectTabletAlias, err = wr.chooseNewMain(ctx, shardInfo, tabletMap, avoidMainTabletAlias, waitReplicasTimeout)
		if err != nil {
			return err
		}
		if mainElectTabletAlias == nil {
			return fmt.Errorf("cannot find a tablet to reparent to")
		}
		wr.logger.Infof("elected new main candidate %v", topoproto.TabletAliasString(mainElectTabletAlias))
		event.DispatchUpdate(ev, "elected new main candidate")
	}
	mainElectTabletAliasStr := topoproto.TabletAliasString(mainElectTabletAlias)
	mainElectTabletInfo, ok := tabletMap[mainElectTabletAliasStr]
	if !ok {
		return fmt.Errorf("main-elect tablet %v is not in the shard", mainElectTabletAliasStr)
	}
	ev.NewMain = *mainElectTabletInfo.Tablet
	if topoproto.TabletAliasIsZero(shardInfo.MainAlias) {
		return fmt.Errorf("the shard has no main, use EmergencyReparentShard")
	}

	// Find the current main (if any) based on the tablet states. We no longer
	// trust the shard record for this, because it is updated asynchronously.
	currentMain := wr.findCurrentMain(tabletMap)

	var reparentJournalPos string

	if currentMain == nil {
		// We don't know who the current main is. Either there is no current
		// main at all (no tablet claims to be MASTER), or there is no clear
		// winner (multiple MASTER tablets with the same timestamp).
		// Check if it's safe to promote the selected main candidate.
		wr.logger.Infof("No clear winner found for current main term; checking if it's safe to recover by electing %v", mainElectTabletAliasStr)

		// As we contact each tablet, we'll send its replication position here.
		type tabletPos struct {
			tabletAliasStr string
			tablet         *topodatapb.Tablet
			pos            mysql.Position
		}
		positions := make(chan tabletPos, len(tabletMap))

		// First stop the world, to ensure no writes are happening anywhere.
		// Since we don't trust that we know which tablets might be acting as
		// mains, we simply demote everyone.
		//
		// Unlike the normal, single-main case, we don't try to undo this if
		// we bail out. If we're here, it means there is no clear main, so we
		// don't know that it's safe to roll back to the previous state.
		// Leaving everything read-only is probably safer than whatever weird
		// state we were in before.
		//
		// If any tablets are unreachable, we can't be sure it's safe, because
		// one of the unreachable ones might have a replication position farther
		// ahead than the candidate main.
		wgStopAll := sync.WaitGroup{}
		rec := concurrency.AllErrorRecorder{}

		stopAllCtx, stopAllCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer stopAllCancel()

		for tabletAliasStr, tablet := range tabletMap {
			wgStopAll.Add(1)
			go func(tabletAliasStr string, tablet *topodatapb.Tablet) {
				defer wgStopAll.Done()

				// Regardless of what type this tablet thinks it is, we always
				// call DemoteMain to ensure the underlying MySQL is read-only
				// and to check its replication position. DemoteMain is
				// idempotent so it's fine to call it on a replica that's
				// already read-only.
				wr.logger.Infof("demote tablet %v", tabletAliasStr)
				posStr, err := wr.tmc.DemoteMain(stopAllCtx, tablet)
				if err != nil {
					rec.RecordError(vterrors.Wrapf(err, "DemoteMain failed on contested main %v", tabletAliasStr))
					return
				}
				pos, err := mysql.DecodePosition(posStr)
				if err != nil {
					rec.RecordError(vterrors.Wrapf(err, "can't decode replication position for tablet %v", tabletAliasStr))
					return
				}
				positions <- tabletPos{
					tabletAliasStr: tabletAliasStr,
					tablet:         tablet,
					pos:            pos,
				}
			}(tabletAliasStr, tablet.Tablet)
		}
		wgStopAll.Wait()
		close(positions)
		if rec.HasErrors() {
			return vterrors.Wrap(rec.Error(), "failed to demote all tablets")
		}

		// Make a map of tablet positions.
		tabletPosMap := make(map[string]tabletPos, len(tabletMap))
		for tp := range positions {
			tabletPosMap[tp.tabletAliasStr] = tp
		}

		// Make sure no tablet has a replication position farther ahead than the
		// candidate main. It's up to our caller to choose a suitable
		// candidate, and to choose another one if this check fails.
		//
		// Note that we still allow replication to run during this time, but we
		// assume that no new high water mark can appear because we demoted all
		// tablets to read-only.
		//
		// TODO: Consider temporarily replicating from another tablet to catch up.
		tp, ok := tabletPosMap[mainElectTabletAliasStr]
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "main-elect tablet %v not found in tablet map", mainElectTabletAliasStr)
		}
		mainElectPos := tp.pos
		for _, tp := range tabletPosMap {
			// The main elect pos has to be at least as far as every tablet.
			if !mainElectPos.AtLeast(tp.pos) {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "tablet %v position (%v) contains transactions not found in main-elect %v position (%v)",
					tp.tabletAliasStr, tp.pos, mainElectTabletAliasStr, mainElectPos)
			}
		}

		// Check we still have the topology lock.
		if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
			return vterrors.Wrap(err, "lost topology lock; aborting")
		}

		// Promote the selected candidate to main.
		promoteCtx, promoteCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer promoteCancel()
		rp, err := wr.tmc.PromoteReplica(promoteCtx, mainElectTabletInfo.Tablet)
		if err != nil {
			return vterrors.Wrapf(err, "failed to promote %v to main", mainElectTabletAliasStr)
		}
		reparentJournalPos = rp
	} else if topoproto.TabletAliasEqual(currentMain.Alias, mainElectTabletAlias) {
		// It is possible that a previous attempt to reparent failed to SetReadWrite
		// so call it here to make sure underlying mysql is ReadWrite
		rwCtx, rwCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer rwCancel()

		if err := wr.tmc.SetReadWrite(rwCtx, mainElectTabletInfo.Tablet); err != nil {
			return vterrors.Wrapf(err, "failed to SetReadWrite on current main %v", mainElectTabletAliasStr)
		}
		// The main is already the one we want according to its tablet record.
		// Refresh it to make sure the tablet has read its record recently.
		refreshCtx, refreshCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer refreshCancel()

		if err := wr.tmc.RefreshState(refreshCtx, mainElectTabletInfo.Tablet); err != nil {
			return vterrors.Wrapf(err, "failed to RefreshState on current main %v", mainElectTabletAliasStr)
		}

		// Then get the position so we can try to fix replicas (below).
		rp, err := wr.tmc.MainPosition(refreshCtx, mainElectTabletInfo.Tablet)
		if err != nil {
			return vterrors.Wrapf(err, "failed to get replication position of current main %v", mainElectTabletAliasStr)
		}
		reparentJournalPos = rp
	} else {
		// There is already a main and it's not the one we want.
		oldMainTabletInfo := currentMain
		ev.OldMain = *oldMainTabletInfo.Tablet

		// Before demoting the old main, first make sure replication is
		// working from the old main to the candidate main. If it's not
		// working, we can't do a planned reparent because the candidate won't
		// catch up.
		wr.logger.Infof("Checking replication on main-elect %v", mainElectTabletAliasStr)

		// First we find the position of the current main. Note that this is
		// just a snapshot of the position since we let it keep accepting new
		// writes until we're sure we're going to proceed.
		snapshotCtx, snapshotCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer snapshotCancel()

		snapshotPos, err := wr.tmc.MainPosition(snapshotCtx, currentMain.Tablet)
		if err != nil {
			return vterrors.Wrapf(err, "can't get replication position on current main %v; current main must be healthy to perform planned reparent", currentMain.AliasString())
		}

		// Now wait for the main-elect to catch up to that snapshot point.
		// If it catches up to that point within the waitReplicasTimeout,
		// we can be fairly confident it will catch up on everything that's
		// happened in the meantime once we demote the main to stop writes.
		//
		// We do this as an idempotent SetMain to make sure the replica knows
		// who the current main is.
		setMainCtx, setMainCancel := context.WithTimeout(ctx, waitReplicasTimeout)
		defer setMainCancel()

		err = wr.tmc.SetMain(setMainCtx, mainElectTabletInfo.Tablet, currentMain.Alias, 0, snapshotPos, true)
		if err != nil {
			return vterrors.Wrapf(err, "replication on main-elect %v did not catch up in time; replication must be healthy to perform planned reparent", mainElectTabletAliasStr)
		}

		// Check we still have the topology lock.
		if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
			return vterrors.Wrap(err, "lost topology lock; aborting")
		}

		// Demote the old main and get its replication position. It's fine if
		// the old main was already demoted, since DemoteMain is idempotent.
		wr.logger.Infof("demote current main %v", oldMainTabletInfo.Alias)
		event.DispatchUpdate(ev, "demoting old main")

		demoteCtx, demoteCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer demoteCancel()

		rp, err := wr.tmc.DemoteMain(demoteCtx, oldMainTabletInfo.Tablet)
		if err != nil {
			return fmt.Errorf("old main tablet %v DemoteMain failed: %v", topoproto.TabletAliasString(shardInfo.MainAlias), err)
		}

		waitCtx, waitCancel := context.WithTimeout(ctx, waitReplicasTimeout)
		defer waitCancel()

		waitErr := wr.tmc.WaitForPosition(waitCtx, mainElectTabletInfo.Tablet, rp)
		if waitErr != nil || ctx.Err() == context.DeadlineExceeded {
			// If the new main fails to catch up within the timeout,
			// we try to roll back to the original main before aborting.
			// It is possible that we have used up the original context, or that
			// not enough time is left on it before it times out.
			// But at this point we really need to be able to Undo so as not to
			// leave the cluster in a bad state.
			// So we create a fresh context based on context.Background().
			undoCtx, undoCancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
			defer undoCancel()
			if undoErr := wr.tmc.UndoDemoteMain(undoCtx, oldMainTabletInfo.Tablet); undoErr != nil {
				log.Warningf("Encountered error while trying to undo DemoteMain: %v", undoErr)
			}
			if waitErr != nil {
				return vterrors.Wrapf(err, "main-elect tablet %v failed to catch up with replication", mainElectTabletAliasStr)
			}
			return vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "PlannedReparent timed out, please try again.")
		}

		promoteCtx, promoteCancel := context.WithTimeout(ctx, waitReplicasTimeout)
		defer promoteCancel()
		rp, err = wr.tmc.PromoteReplica(promoteCtx, mainElectTabletInfo.Tablet)
		if err != nil {
			return vterrors.Wrapf(err, "main-elect tablet %v failed to be upgraded to main - please try again", mainElectTabletAliasStr)
		}

		if ctx.Err() == context.DeadlineExceeded {
			// PromoteReplica succeeded but the context has expired. PRS needs to be re-run to complete
			return vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "PlannedReparent timed out after promoting new main. Please re-run to fixup replicas.")
		}
		reparentJournalPos = rp
	}

	// Check we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, "lost topology lock, aborting")
	}

	// Create a cancelable context for the following RPCs.
	// If error conditions happen, we can cancel all outgoing RPCs.
	replCtx, replCancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer replCancel()

	// Go through all the tablets:
	// - new main: populate the reparent journal
	// - everybody else: reparent to new main, wait for row
	event.DispatchUpdate(ev, "reparenting all tablets")

	// We add a (hopefully) unique record to the reparent journal table on the
	// new main so we can check if replicas got it through replication.
	reparentJournalTimestamp := time.Now().UnixNano()

	// Point all replicas at the new main and check that they receive the
	// reparent journal entry, proving they are replicating from the new main.
	// We do this concurrently with adding the journal entry (below), because
	// if semi-sync is enabled, the update to the journal table can't succeed
	// until at least one replica is successfully attached to the new main.
	wgReplicas := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for alias, tabletInfo := range tabletMap {
		if alias == mainElectTabletAliasStr {
			continue
		}
		wgReplicas.Add(1)
		go func(alias string, tabletInfo *topo.TabletInfo) {
			defer wgReplicas.Done()
			wr.logger.Infof("setting new main on replica %v", alias)

			// We used to force subordinate start on the old main, but now that
			// we support "resuming" a PRS attempt that failed, we can no
			// longer assume that we know who the old main was.
			// Instead, we rely on the old main to remember that it needs
			// to start replication after being converted to a replica.
			forceStartReplication := false

			if err := wr.tmc.SetMain(replCtx, tabletInfo.Tablet, mainElectTabletAlias, reparentJournalTimestamp, "", forceStartReplication); err != nil {
				rec.RecordError(fmt.Errorf("tablet %v SetMain failed: %v", alias, err))
				return
			}
		}(alias, tabletInfo)
	}

	// Add a reparent journal entry on the new main.
	wr.logger.Infof("populating reparent journal on new main %v", mainElectTabletAliasStr)
	err = wr.tmc.PopulateReparentJournal(replCtx, mainElectTabletInfo.Tablet, reparentJournalTimestamp, plannedReparentShardOperation, mainElectTabletAlias, reparentJournalPos)
	if err != nil {
		// The main failed. There's no way the replicas will work, so cancel them all.
		wr.logger.Warningf("main failed to PopulateReparentJournal, canceling replica reparent attempts")
		replCancel()
		wgReplicas.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on main: %v", err)
	}

	// Wait for the replicas to complete.
	wgReplicas.Wait()
	if err := rec.Error(); err != nil {
		wr.Logger().Errorf2(err, "some replicas failed to reparent; retry PlannedReparentShard with the same new main alias to retry failed replicas")
		return err
	}

	return nil
}

// findCurrentMain returns the current main of a shard, if any.
//
// The tabletMap must be a complete map (not a partial result) for the shard.
//
// The current main is whichever MASTER tablet (if any) has the highest
// MainTermStartTime, which is the same rule that vtgate uses to route main
// traffic.
//
// The return value is nil if the current main can't be definitively
// determined. This can happen either if no tablet claims to be MASTER, or if
// multiple MASTER tablets claim to have the same timestamp (a tie).
func (wr *Wrangler) findCurrentMain(tabletMap map[string]*topo.TabletInfo) *topo.TabletInfo {
	var currentMain *topo.TabletInfo
	var currentMainTime time.Time

	for _, tablet := range tabletMap {
		// Only look at mains.
		if tablet.Type != topodatapb.TabletType_MASTER {
			continue
		}
		// Fill in first main we find.
		if currentMain == nil {
			currentMain = tablet
			currentMainTime = tablet.GetMainTermStartTime()
			continue
		}
		// If we find any other mains, compare timestamps.
		newMainTime := tablet.GetMainTermStartTime()
		if newMainTime.After(currentMainTime) {
			currentMain = tablet
			currentMainTime = newMainTime
			continue
		}
		if newMainTime.Equal(currentMainTime) {
			// A tie shouldn't happen unless the upgrade order was violated
			// (some vttablets have not yet been upgraded) or if we get really
			// unlucky. However, if it does happen, we need to be safe and not
			// assume we know who the true main is.
			wr.logger.Warningf("Multiple mains (%v and %v) are tied for MainTermStartTime; can't determine the true main.",
				topoproto.TabletAliasString(currentMain.Alias),
				topoproto.TabletAliasString(tablet.Alias))
			return nil
		}
	}

	return currentMain
}

// maxReplPosSearch is a struct helping to search for a tablet with the largest replication
// position querying status from all tablets in parallel.
type maxReplPosSearch struct {
	wrangler            *Wrangler
	ctx                 context.Context
	waitReplicasTimeout time.Duration
	waitGroup           sync.WaitGroup
	maxPosLock          sync.Mutex
	maxPos              mysql.Position
	maxPosTablet        *topodatapb.Tablet
}

func (maxPosSearch *maxReplPosSearch) processTablet(tablet *topodatapb.Tablet) {
	defer maxPosSearch.waitGroup.Done()
	maxPosSearch.wrangler.logger.Infof("getting replication position from %v", topoproto.TabletAliasString(tablet.Alias))

	subordinateStatusCtx, cancelSubordinateStatus := context.WithTimeout(maxPosSearch.ctx, maxPosSearch.waitReplicasTimeout)
	defer cancelSubordinateStatus()

	status, err := maxPosSearch.wrangler.tmc.SubordinateStatus(subordinateStatusCtx, tablet)
	if err != nil {
		maxPosSearch.wrangler.logger.Warningf("failed to get replication status from %v, ignoring tablet: %v", topoproto.TabletAliasString(tablet.Alias), err)
		return
	}
	replPos, err := mysql.DecodePosition(status.Position)
	if err != nil {
		maxPosSearch.wrangler.logger.Warningf("cannot decode subordinate %v position %v: %v", topoproto.TabletAliasString(tablet.Alias), status.Position, err)
		return
	}

	maxPosSearch.maxPosLock.Lock()
	if maxPosSearch.maxPosTablet == nil || !maxPosSearch.maxPos.AtLeast(replPos) {
		maxPosSearch.maxPos = replPos
		maxPosSearch.maxPosTablet = tablet
	}
	maxPosSearch.maxPosLock.Unlock()
}

// chooseNewMain finds a tablet that is going to become main after reparent. The criteria
// for the new main-elect are (preferably) to be in the same cell as the current main, and
// to be different from avoidMainTabletAlias. The tablet with the largest replication
// position is chosen to minimize the time of catching up with the main. Note that the search
// for largest replication position will race with transactions being executed on the main at
// the same time, so when all tablets are roughly at the same position then the choice of the
// new main-elect will be somewhat unpredictable.
func (wr *Wrangler) chooseNewMain(
	ctx context.Context,
	shardInfo *topo.ShardInfo,
	tabletMap map[string]*topo.TabletInfo,
	avoidMainTabletAlias *topodatapb.TabletAlias,
	waitReplicasTimeout time.Duration) (*topodatapb.TabletAlias, error) {

	if avoidMainTabletAlias == nil {
		return nil, fmt.Errorf("tablet to avoid for reparent is not provided, cannot choose new main")
	}
	var mainCell string
	if shardInfo.MainAlias != nil {
		mainCell = shardInfo.MainAlias.Cell
	}

	maxPosSearch := maxReplPosSearch{
		wrangler:            wr,
		ctx:                 ctx,
		waitReplicasTimeout: waitReplicasTimeout,
		waitGroup:           sync.WaitGroup{},
		maxPosLock:          sync.Mutex{},
	}
	for _, tabletInfo := range tabletMap {
		if (mainCell != "" && tabletInfo.Alias.Cell != mainCell) ||
			topoproto.TabletAliasEqual(tabletInfo.Alias, avoidMainTabletAlias) ||
			tabletInfo.Tablet.Type != topodatapb.TabletType_REPLICA {
			continue
		}
		maxPosSearch.waitGroup.Add(1)
		go maxPosSearch.processTablet(tabletInfo.Tablet)
	}
	maxPosSearch.waitGroup.Wait()

	if maxPosSearch.maxPosTablet == nil {
		return nil, nil
	}
	return maxPosSearch.maxPosTablet.Alias, nil
}

// EmergencyReparentShard will make the provided tablet the main for
// the shard, when the old main is completely unreachable.
func (wr *Wrangler) EmergencyReparentShard(ctx context.Context, keyspace, shard string, mainElectTabletAlias *topodatapb.TabletAlias, waitReplicasTimeout time.Duration) (err error) {
	// lock the shard
	ctx, unlock, lockErr := wr.ts.LockShard(ctx, keyspace, shard, fmt.Sprintf("EmergencyReparentShard(%v)", topoproto.TabletAliasString(mainElectTabletAlias)))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// do the work
	err = wr.emergencyReparentShardLocked(ctx, ev, keyspace, shard, mainElectTabletAlias, waitReplicasTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed EmergencyReparentShard: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished EmergencyReparentShard")
	}
	return err
}

func (wr *Wrangler) emergencyReparentShardLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, mainElectTabletAlias *topodatapb.TabletAlias, waitReplicasTimeout time.Duration) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading all tablets")
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// Check invariants we're going to depend on.
	mainElectTabletAliasStr := topoproto.TabletAliasString(mainElectTabletAlias)
	mainElectTabletInfo, ok := tabletMap[mainElectTabletAliasStr]
	if !ok {
		return fmt.Errorf("main-elect tablet %v is not in the shard", mainElectTabletAliasStr)
	}
	ev.NewMain = *mainElectTabletInfo.Tablet
	if topoproto.TabletAliasEqual(shardInfo.MainAlias, mainElectTabletAlias) {
		return fmt.Errorf("main-elect tablet %v is already the main", topoproto.TabletAliasString(mainElectTabletAlias))
	}

	// Deal with the old main: try to remote-scrap it, if it's
	// truly dead we force-scrap it. Remove it from our map in any case.
	if shardInfo.HasMain() {
		deleteOldMain := true
		shardInfoMainAliasStr := topoproto.TabletAliasString(shardInfo.MainAlias)
		oldMainTabletInfo, ok := tabletMap[shardInfoMainAliasStr]
		if ok {
			delete(tabletMap, shardInfoMainAliasStr)
		} else {
			oldMainTabletInfo, err = wr.ts.GetTablet(ctx, shardInfo.MainAlias)
			if err != nil {
				wr.logger.Warningf("cannot read old main tablet %v, won't touch it: %v", shardInfoMainAliasStr, err)
				deleteOldMain = false
			}
		}

		if deleteOldMain {
			ev.OldMain = *oldMainTabletInfo.Tablet
			wr.logger.Infof("deleting old main %v", shardInfoMainAliasStr)

			ctx, cancel := context.WithTimeout(ctx, waitReplicasTimeout)
			defer cancel()

			if err := topotools.DeleteTablet(ctx, wr.ts, oldMainTabletInfo.Tablet); err != nil {
				wr.logger.Warningf("failed to delete old main tablet %v: %v", shardInfoMainAliasStr, err)
			}
		}
	}

	// Stop replication on all subordinates, get their current
	// replication position
	event.DispatchUpdate(ev, "stop replication on all subordinates")
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	statusMap := make(map[string]*replicationdatapb.Status)
	for alias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(alias string, tabletInfo *topo.TabletInfo) {
			defer wg.Done()
			wr.logger.Infof("getting replication position from %v", alias)
			ctx, cancel := context.WithTimeout(ctx, waitReplicasTimeout)
			defer cancel()
			rp, err := wr.tmc.StopReplicationAndGetStatus(ctx, tabletInfo.Tablet)
			if err != nil {
				wr.logger.Warningf("failed to get replication status from %v, ignoring tablet: %v", alias, err)
				return
			}
			mu.Lock()
			statusMap[alias] = rp
			mu.Unlock()
		}(alias, tabletInfo)
	}
	wg.Wait()

	// Check we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Verify mainElect is alive and has the most advanced position
	mainElectStatus, ok := statusMap[mainElectTabletAliasStr]
	if !ok {
		return fmt.Errorf("couldn't get main elect %v replication position", topoproto.TabletAliasString(mainElectTabletAlias))
	}
	mainElectPos, err := mysql.DecodePosition(mainElectStatus.Position)
	if err != nil {
		return fmt.Errorf("cannot decode main elect position %v: %v", mainElectStatus.Position, err)
	}
	for alias, status := range statusMap {
		if alias == mainElectTabletAliasStr {
			continue
		}
		pos, err := mysql.DecodePosition(status.Position)
		if err != nil {
			return fmt.Errorf("cannot decode replica %v position %v: %v", alias, status.Position, err)
		}
		if !mainElectPos.AtLeast(pos) {
			return fmt.Errorf("tablet %v is more advanced than main elect tablet %v: %v > %v", alias, mainElectTabletAliasStr, status.Position, mainElectStatus.Position)
		}
	}

	// Promote the mainElect
	wr.logger.Infof("promote tablet %v to main", topoproto.TabletAliasString(mainElectTabletAlias))
	event.DispatchUpdate(ev, "promoting replica")
	rp, err := wr.tmc.PromoteReplica(ctx, mainElectTabletInfo.Tablet)
	if err != nil {
		return fmt.Errorf("main-elect tablet %v failed to be upgraded to main: %v", topoproto.TabletAliasString(mainElectTabletAlias), err)
	}

	// Check we stil have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Create a cancelable context for the following RPCs.
	// If error conditions happen, we can cancel all outgoing RPCs.
	replCtx, replCancel := context.WithCancel(ctx)
	defer replCancel()

	// Reset replication on all subordinates to point to the new main, and
	// insert test row in the new main.
	// Go through all the tablets:
	// - new main: populate the reparent journal
	// - everybody else: reparent to new main, wait for row
	event.DispatchUpdate(ev, "reparenting all tablets")
	now := time.Now().UnixNano()
	wgMain := sync.WaitGroup{}
	wgSubordinates := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	var mainErr error
	for alias, tabletInfo := range tabletMap {
		if alias == mainElectTabletAliasStr {
			wgMain.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgMain.Done()
				wr.logger.Infof("populating reparent journal on new main %v", alias)
				mainErr = wr.tmc.PopulateReparentJournal(replCtx, tabletInfo.Tablet, now, emergencyReparentShardOperation, mainElectTabletAlias, rp)
			}(alias, tabletInfo)
		} else {
			wgSubordinates.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgSubordinates.Done()
				wr.logger.Infof("setting new main on subordinate %v", alias)
				forceStartSubordinate := false
				if status, ok := statusMap[alias]; ok {
					forceStartSubordinate = status.SubordinateIoRunning || status.SubordinateSqlRunning
				}
				if err := wr.tmc.SetMain(replCtx, tabletInfo.Tablet, mainElectTabletAlias, now, "", forceStartSubordinate); err != nil {
					rec.RecordError(fmt.Errorf("tablet %v SetMain failed: %v", alias, err))
				}
			}(alias, tabletInfo)
		}
	}

	wgMain.Wait()
	if mainErr != nil {
		// The main failed, there is no way the
		// subordinates will work.  So we cancel them all.
		wr.logger.Warningf("main failed to PopulateReparentJournal, canceling subordinates")
		replCancel()
		wgSubordinates.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on main: %v", mainErr)
	}

	// Wait for the subordinates to complete. If some of them fail, we
	// will rebuild the shard serving graph anyway
	wgSubordinates.Wait()
	if err := rec.Error(); err != nil {
		wr.Logger().Errorf2(err, "some subordinates failed to reparent")
		return err
	}

	return nil
}

// TabletExternallyReparented changes the type of new main for this shard to MASTER
// and updates it's tablet record in the topo. Updating the shard record is handled
// by the new main tablet
func (wr *Wrangler) TabletExternallyReparented(ctx context.Context, newMainAlias *topodatapb.TabletAlias) error {

	tabletInfo, err := wr.ts.GetTablet(ctx, newMainAlias)
	if err != nil {
		log.Warningf("TabletExternallyReparented: failed to read tablet record for %v: %v", newMainAlias, err)
		return err
	}

	// Check the global shard record.
	tablet := tabletInfo.Tablet
	si, err := wr.ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		log.Warningf("TabletExternallyReparented: failed to read global shard record for %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
		return err
	}

	// We update the tablet only if it is not currently main
	if tablet.Type != topodatapb.TabletType_MASTER {
		log.Infof("TabletExternallyReparented: executing tablet type change to MASTER")

		// Create a reusable Reparent event with available info.
		ev := &events.Reparent{
			ShardInfo: *si,
			NewMain: *tablet,
			OldMain: topodatapb.Tablet{
				Alias: si.MainAlias,
				Type:  topodatapb.TabletType_MASTER,
			},
		}
		defer func() {
			if err != nil {
				event.DispatchUpdate(ev, "failed: "+err.Error())
			}
		}()
		event.DispatchUpdate(ev, "starting external reparent")

		if err := wr.tmc.ChangeType(ctx, tablet, topodatapb.TabletType_MASTER); err != nil {
			log.Warningf("Error calling ChangeType on new main %v: %v", topoproto.TabletAliasString(newMainAlias), err)
			return err
		}
		event.DispatchUpdate(ev, "finished")
	}
	return nil
}
