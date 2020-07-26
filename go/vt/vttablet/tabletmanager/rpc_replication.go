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
	"flag"
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	enableSemiSync   = flag.Bool("enable_semi_sync", false, "Enable semi-sync when configuring replication, on main and replica tablets only (rdonly tablets will not ack).")
	setSuperReadOnly = flag.Bool("use_super_read_only", false, "Set super_read_only flag when performing planned failover.")
)

// SubordinateStatus returns the replication status
func (agent *ActionAgent) SubordinateStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	status, err := agent.MysqlDaemon.SubordinateStatus()
	if err != nil {
		return nil, err
	}
	return mysql.SubordinateStatusToProto(status), nil
}

// MainPosition returns the main position
func (agent *ActionAgent) MainPosition(ctx context.Context) (string, error) {
	pos, err := agent.MysqlDaemon.MainPosition()
	if err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// WaitForPosition returns the main position
func (agent *ActionAgent) WaitForPosition(ctx context.Context, pos string) error {
	mpos, err := mysql.DecodePosition(pos)
	if err != nil {
		return err
	}
	return agent.MysqlDaemon.WaitMainPos(ctx, mpos)
}

// StopSubordinate will stop the mysql. Works both when Vitess manages
// replication or not (using hook if not).
func (agent *ActionAgent) StopSubordinate(ctx context.Context) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	return agent.stopSubordinateLocked(ctx)
}

func (agent *ActionAgent) stopSubordinateLocked(ctx context.Context) error {

	// Remember that we were told to stop, so we don't try to
	// restart ourselves (in replication_reporter).
	agent.setSubordinateStopped(true)

	// Also tell Orchestrator we're stopped on purpose for some Vitess task.
	// Do this in the background, as it's best-effort.
	go func() {
		if agent.orc == nil {
			return
		}
		if err := agent.orc.BeginMaintenance(agent.Tablet(), "vttablet has been told to StopSubordinate"); err != nil {
			log.Warningf("Orchestrator BeginMaintenance failed: %v", err)
		}
	}()

	return agent.MysqlDaemon.StopSubordinate(agent.hookExtraEnv())
}

// StopSubordinateMinimum will stop the subordinate after it reaches at least the
// provided position. Works both when Vitess manages
// replication or not (using hook if not).
func (agent *ActionAgent) StopSubordinateMinimum(ctx context.Context, position string, waitTime time.Duration) (string, error) {
	if err := agent.lock(ctx); err != nil {
		return "", err
	}
	defer agent.unlock()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return "", err
	}
	waitCtx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()
	if err := agent.MysqlDaemon.WaitMainPos(waitCtx, pos); err != nil {
		return "", err
	}
	if err := agent.stopSubordinateLocked(ctx); err != nil {
		return "", err
	}
	pos, err = agent.MysqlDaemon.MainPosition()
	if err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// StartSubordinate will start the mysql. Works both when Vitess manages
// replication or not (using hook if not).
func (agent *ActionAgent) StartSubordinate(ctx context.Context) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	agent.setSubordinateStopped(false)

	// Tell Orchestrator we're no longer stopped on purpose.
	// Do this in the background, as it's best-effort.
	go func() {
		if agent.orc == nil {
			return
		}
		if err := agent.orc.EndMaintenance(agent.Tablet()); err != nil {
			log.Warningf("Orchestrator EndMaintenance failed: %v", err)
		}
	}()

	if err := agent.fixSemiSync(agent.Tablet().Type); err != nil {
		return err
	}
	return agent.MysqlDaemon.StartSubordinate(agent.hookExtraEnv())
}

// StartSubordinateUntilAfter will start the replication and let it catch up
// until and including the transactions in `position`
func (agent *ActionAgent) StartSubordinateUntilAfter(ctx context.Context, position string, waitTime time.Duration) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	waitCtx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}

	return agent.MysqlDaemon.StartSubordinateUntilAfter(waitCtx, pos)
}

// GetSubordinates returns the address of all the subordinates
func (agent *ActionAgent) GetSubordinates(ctx context.Context) ([]string, error) {
	return mysqlctl.FindSubordinates(agent.MysqlDaemon)
}

// ResetReplication completely resets the replication on the host.
// All binary and relay logs are flushed. All replication positions are reset.
func (agent *ActionAgent) ResetReplication(ctx context.Context) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	agent.setSubordinateStopped(true)
	return agent.MysqlDaemon.ResetReplication(ctx)
}

// InitMain enables writes and returns the replication position.
func (agent *ActionAgent) InitMain(ctx context.Context) (string, error) {
	if err := agent.lock(ctx); err != nil {
		return "", err
	}
	defer agent.unlock()

	// Initializing as main implies undoing any previous "do not replicate".
	agent.setSubordinateStopped(false)

	// we need to insert something in the binlogs, so we can get the
	// current position. Let's just use the mysqlctl.CreateReparentJournal commands.
	cmds := mysqlctl.CreateReparentJournal()
	if err := agent.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds); err != nil {
		return "", err
	}

	// get the current replication position
	pos, err := agent.MysqlDaemon.MainPosition()
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if err := agent.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
		return "", err
	}

	// Set the server read-write, from now on we can accept real
	// client writes. Note that if semi-sync replication is enabled,
	// we'll still need some subordinates to be able to commit transactions.
	startTime := time.Now()
	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return "", err
	}

	// Change our type to main if not already
	_, err = topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topodatapb.TabletType_MASTER, logutil.TimeToProto(startTime))
	if err != nil {
		return "", err
	}
	// We only update agent's mainTermStartTime if we were able to update the topo.
	// This ensures that in case of a failure, we are never in a situation where the
	// tablet's timestamp is ahead of the topo's timestamp.
	agent.setMainTermStartTime(startTime)
	// and refresh our state
	agent.initReplication = true
	if err := agent.refreshTablet(ctx, "InitMain"); err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// PopulateReparentJournal adds an entry into the reparent_journal table.
func (agent *ActionAgent) PopulateReparentJournal(ctx context.Context, timeCreatedNS int64, actionName string, mainAlias *topodatapb.TabletAlias, position string) error {
	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}
	cmds := mysqlctl.CreateReparentJournal()
	cmds = append(cmds, mysqlctl.PopulateReparentJournal(timeCreatedNS, actionName, topoproto.TabletAliasString(mainAlias), pos))

	return agent.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds)
}

// InitSubordinate sets replication main and position, and waits for the
// reparent_journal table entry up to context timeout
func (agent *ActionAgent) InitSubordinate(ctx context.Context, parent *topodatapb.TabletAlias, position string, timeCreatedNS int64) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return err
	}
	ti, err := agent.TopoServer.GetTablet(ctx, parent)
	if err != nil {
		return err
	}

	agent.setSubordinateStopped(false)

	// If using semi-sync, we need to enable it before connecting to main.
	// If we were a main type, we need to switch back to replica settings.
	// Otherwise we won't be able to commit anything.
	tt := agent.Tablet().Type
	if tt == topodatapb.TabletType_MASTER {
		tt = topodatapb.TabletType_REPLICA
	}
	if err := agent.fixSemiSync(tt); err != nil {
		return err
	}

	if err := agent.MysqlDaemon.SetSubordinatePosition(ctx, pos); err != nil {
		return err
	}
	if err := agent.MysqlDaemon.SetMain(ctx, topoproto.MysqlHostname(ti.Tablet), int(topoproto.MysqlPort(ti.Tablet)), false /* subordinateStopBefore */, true /* subordinateStartAfter */); err != nil {
		return err
	}
	agent.initReplication = true

	// If we were a main type, switch our type to replica.  This
	// is used on the old main when using InitShardMain with
	// -force, and the new main is different from the old main.
	if agent.Tablet().Type == topodatapb.TabletType_MASTER {
		if _, err := topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topodatapb.TabletType_REPLICA, nil); err != nil {
			return err
		}

		if err := agent.refreshTablet(ctx, "InitSubordinate"); err != nil {
			return err
		}
	}

	// wait until we get the replicated row, or our context times out
	return agent.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS)
}

// DemoteMain prepares a MASTER tablet to give up mainship to another tablet.
//
// It attemps to idempotently ensure the following guarantees upon returning
// successfully:
//   * No future writes will be accepted.
//   * No writes are in-flight.
//   * MySQL is in read-only mode.
//   * Semi-sync settings are consistent with a REPLICA tablet.
//
// If necessary, it waits for all in-flight writes to complete or time out.
//
// It should be safe to call this on a MASTER tablet that was already demoted,
// or on a tablet that already transitioned to REPLICA.
//
// If a step fails in the middle, it will try to undo any changes it made.
func (agent *ActionAgent) DemoteMain(ctx context.Context) (string, error) {
	// The public version always reverts on partial failure.
	return agent.demoteMain(ctx, true /* revertPartialFailure */)
}

// demoteMain implements DemoteMain with an additional, private option.
//
// If revertPartialFailure is true, and a step fails in the middle, it will try
// to undo any changes it made.
func (agent *ActionAgent) demoteMain(ctx context.Context, revertPartialFailure bool) (replicationPosition string, finalErr error) {
	if err := agent.lock(ctx); err != nil {
		return "", err
	}
	defer agent.unlock()

	tablet := agent.Tablet()
	wasMain := tablet.Type == topodatapb.TabletType_MASTER
	wasServing := agent.QueryServiceControl.IsServing()
	wasReadOnly, err := agent.MysqlDaemon.IsReadOnly()
	if err != nil {
		return "", err
	}

	// If we are a main tablet and not yet read-only, stop accepting new
	// queries and wait for in-flight queries to complete. If we are not main,
	// or if we are already read-only, there's no need to stop the queryservice
	// in order to ensure the guarantee we are being asked to provide, which is
	// that no writes are occurring.
	if wasMain && !wasReadOnly {
		// Tell Orchestrator we're stopped on purpose for demotion.
		// This is a best effort task, so run it in a goroutine.
		go func() {
			if agent.orc == nil {
				return
			}
			if err := agent.orc.BeginMaintenance(agent.Tablet(), "vttablet has been told to DemoteMain"); err != nil {
				log.Warningf("Orchestrator BeginMaintenance failed: %v", err)
			}
		}()

		// Note that this may block until the transaction timeout if clients
		// don't finish their transactions in time. Even if some transactions
		// have to be killed at the end of their timeout, this will be
		// considered successful. If we are already not serving, this will be
		// idempotent.
		log.Infof("DemoteMain disabling query service")
		if _ /* state changed */, err := agent.QueryServiceControl.SetServingType(tablet.Type, false, nil); err != nil {
			return "", vterrors.Wrap(err, "SetServingType(serving=false) failed")
		}
		defer func() {
			if finalErr != nil && revertPartialFailure && wasServing {
				if _ /* state changed */, err := agent.QueryServiceControl.SetServingType(tablet.Type, true, nil); err != nil {
					log.Warningf("SetServingType(serving=true) failed during revert: %v", err)
				}
			}
		}()
	}

	// Now that we know no writes are in-flight and no new writes can occur,
	// set MySQL to read-only mode. If we are already read-only because of a
	// previous demotion, or because we are not main anyway, this should be
	// idempotent.
	if *setSuperReadOnly {
		// Setting super_read_only also sets read_only
		if err := agent.MysqlDaemon.SetSuperReadOnly(true); err != nil {
			return "", err
		}
	} else {
		if err := agent.MysqlDaemon.SetReadOnly(true); err != nil {
			return "", err
		}
	}
	defer func() {
		if finalErr != nil && revertPartialFailure && !wasReadOnly {
			// setting read_only OFF will also set super_read_only OFF if it was set
			if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
				log.Warningf("SetReadOnly(false) failed during revert: %v", err)
			}
		}
	}()

	// If using semi-sync, we need to disable main-side.
	if err := agent.fixSemiSync(topodatapb.TabletType_REPLICA); err != nil {
		return "", err
	}
	defer func() {
		if finalErr != nil && revertPartialFailure && wasMain {
			// enable main-side semi-sync again
			if err := agent.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
				log.Warningf("fixSemiSync(MASTER) failed during revert: %v", err)
			}
		}
	}()

	// Return the current replication position.
	pos, err := agent.MysqlDaemon.MainPosition()
	if err != nil {
		return "", err
	}
	return mysql.EncodePosition(pos), nil
}

// UndoDemoteMain reverts a previous call to DemoteMain
// it sets read-only to false, fixes semi-sync
// and returns its main position.
func (agent *ActionAgent) UndoDemoteMain(ctx context.Context) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	// If using semi-sync, we need to enable main-side.
	if err := agent.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
		return err
	}

	// Now, set the server read-only false.
	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return err
	}

	// Update serving graph
	tablet := agent.Tablet()
	log.Infof("UndoDemoteMain re-enabling query service")
	if _ /* state changed */, err := agent.QueryServiceControl.SetServingType(tablet.Type, true, nil); err != nil {
		return vterrors.Wrap(err, "SetServingType(serving=true) failed")
	}

	return nil
}

// PromoteSubordinateWhenCaughtUp waits for this subordinate to be caught up on
// replication up to the provided point, and then makes the subordinate the
// shard main.
// Deprecated
func (agent *ActionAgent) PromoteSubordinateWhenCaughtUp(ctx context.Context, position string) (string, error) {
	if err := agent.lock(ctx); err != nil {
		return "", err
	}
	defer agent.unlock()

	pos, err := mysql.DecodePosition(position)
	if err != nil {
		return "", err
	}

	if err := agent.MysqlDaemon.WaitMainPos(ctx, pos); err != nil {
		return "", err
	}

	pos, err = agent.MysqlDaemon.Promote(agent.hookExtraEnv())
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if err := agent.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
		return "", err
	}

	startTime := time.Now()
	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return "", err
	}

	_, err = topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topodatapb.TabletType_MASTER, logutil.TimeToProto(startTime))
	if err != nil {
		return "", err
	}

	// We only update agent's mainTermStartTime if we were able to update the topo.
	// This ensures that in case of a failure, we are never in a situation where the
	// tablet's timestamp is ahead of the topo's timestamp.
	agent.setMainTermStartTime(startTime)

	if err := agent.refreshTablet(ctx, "PromoteSubordinateWhenCaughtUp"); err != nil {
		return "", err
	}

	return mysql.EncodePosition(pos), nil
}

// SubordinateWasPromoted promotes a subordinate to main, no questions asked.
func (agent *ActionAgent) SubordinateWasPromoted(ctx context.Context) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()
	startTime := time.Now()

	if _, err := topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topodatapb.TabletType_MASTER, logutil.TimeToProto(startTime)); err != nil {
		return err
	}
	// We only update agent's mainTermStartTime if we were able to update the topo.
	// This ensures that in case of a failure, we are never in a situation where the
	// tablet's timestamp is ahead of the topo's timestamp.
	agent.setMainTermStartTime(startTime)

	if err := agent.refreshTablet(ctx, "SubordinateWasPromoted"); err != nil {
		return err
	}

	return nil
}

// SetMain sets replication main, and waits for the
// reparent_journal table entry up to context timeout
func (agent *ActionAgent) SetMain(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartSubordinate bool) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	if err := agent.setMainLocked(ctx, parentAlias, timeCreatedNS, waitPosition, forceStartSubordinate); err != nil {
		return err
	}

	// Always refresh the tablet, even if we may not have changed it.
	// It's possible that we changed it earlier but failed to refresh.
	// Note that we do this outside setMainLocked() because this should never
	// be done as part of setMainRepairReplication().
	if err := agent.refreshTablet(ctx, "SetMain"); err != nil {
		return err
	}

	return nil
}

func (agent *ActionAgent) setMainRepairReplication(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartSubordinate bool) (err error) {
	parent, err := agent.TopoServer.GetTablet(ctx, parentAlias)
	if err != nil {
		return err
	}

	ctx, unlock, lockErr := agent.TopoServer.LockShard(ctx, parent.Tablet.GetKeyspace(), parent.Tablet.GetShard(), fmt.Sprintf("repairReplication to %v as parent)", topoproto.TabletAliasString(parentAlias)))
	if lockErr != nil {
		return lockErr
	}

	defer unlock(&err)

	return agent.setMainLocked(ctx, parentAlias, timeCreatedNS, waitPosition, forceStartSubordinate)
}

func (agent *ActionAgent) setMainLocked(ctx context.Context, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartSubordinate bool) (err error) {
	// End orchestrator maintenance at the end of fixing replication.
	// This is a best effort operation, so it should happen in a goroutine
	defer func() {
		go func() {
			if agent.orc == nil {
				return
			}
			if err := agent.orc.EndMaintenance(agent.Tablet()); err != nil {
				log.Warningf("Orchestrator EndMaintenance failed: %v", err)
			}
		}()
	}()

	// Change our type to REPLICA if we used to be MASTER.
	// Being sent SetMain means another MASTER has been successfully promoted,
	// so we convert to REPLICA first, since we want to do it even if other
	// steps fail below.
	// Note it is important to check for MASTER here so that we don't
	// unintentionally change the type of RDONLY tablets
	_, err = agent.TopoServer.UpdateTabletFields(ctx, agent.TabletAlias, func(tablet *topodatapb.Tablet) error {
		if tablet.Type == topodatapb.TabletType_MASTER {
			tablet.Type = topodatapb.TabletType_REPLICA
			tablet.MainTermStartTime = nil
			return nil
		}
		return topo.NewError(topo.NoUpdateNeeded, agent.TabletAlias.String())
	})
	if err != nil {
		return err
	}

	// See if we were replicating at all, and should be replicating.
	wasReplicating := false
	shouldbeReplicating := false
	status, err := agent.MysqlDaemon.SubordinateStatus()
	if err == mysql.ErrNotSubordinate {
		// This is a special error that means we actually succeeded in reading
		// the status, but the status is empty because replication is not
		// configured. We assume this means we used to be a main, so we always
		// try to start replicating once we are told who the new main is.
		shouldbeReplicating = true
		// Since we continue in the case of this error, make sure 'status' is
		// in a known, empty state.
		status = mysql.SubordinateStatus{}
	} else if err != nil {
		// Abort on any other non-nil error.
		return err
	}
	if status.SubordinateIORunning || status.SubordinateSQLRunning {
		wasReplicating = true
		shouldbeReplicating = true
	}
	if forceStartSubordinate {
		shouldbeReplicating = true
	}

	// If using semi-sync, we need to enable it before connecting to main.
	// If we are currently MASTER, assume we are about to become REPLICA.
	tabletType := agent.Tablet().Type
	if tabletType == topodatapb.TabletType_MASTER {
		tabletType = topodatapb.TabletType_REPLICA
	}
	if err := agent.fixSemiSync(tabletType); err != nil {
		return err
	}
	// Update the main address only if needed.
	// We don't want to interrupt replication for no reason.
	parent, err := agent.TopoServer.GetTablet(ctx, parentAlias)
	if err != nil {
		return err
	}
	mainHost := topoproto.MysqlHostname(parent.Tablet)
	mainPort := int(topoproto.MysqlPort(parent.Tablet))
	if status.MainHost != mainHost || status.MainPort != mainPort {
		// This handles both changing the address and starting replication.
		if err := agent.MysqlDaemon.SetMain(ctx, mainHost, mainPort, wasReplicating, shouldbeReplicating); err != nil {
			if err := agent.handleRelayLogError(err); err != nil {
				return err
			}
		}
	} else if shouldbeReplicating {
		// The address is correct. Just start replication if needed.
		if !status.SubordinateRunning() {
			if err := agent.MysqlDaemon.StartSubordinate(agent.hookExtraEnv()); err != nil {
				if err := agent.handleRelayLogError(err); err != nil {
					return err
				}
			}
		}
	}

	// If needed, wait until we replicate to the specified point, or our context
	// times out. Callers can specify the point to wait for as either a
	// GTID-based replication position or a Vitess reparent journal entry,
	// or both.
	if shouldbeReplicating {
		if waitPosition != "" {
			pos, err := mysql.DecodePosition(waitPosition)
			if err != nil {
				return err
			}
			if err := agent.MysqlDaemon.WaitMainPos(ctx, pos); err != nil {
				return err
			}
		}
		if timeCreatedNS != 0 {
			if err := agent.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS); err != nil {
				return err
			}
		}
	}

	return nil
}

// SubordinateWasRestarted updates the parent record for a tablet.
func (agent *ActionAgent) SubordinateWasRestarted(ctx context.Context, parent *topodatapb.TabletAlias) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	// Only change type of former MASTER tablets.
	// Don't change type of RDONLY
	typeChanged := false
	if _, err := agent.TopoServer.UpdateTabletFields(ctx, agent.TabletAlias, func(tablet *topodatapb.Tablet) error {
		if tablet.Type == topodatapb.TabletType_MASTER {
			tablet.Type = topodatapb.TabletType_REPLICA
			tablet.MainTermStartTime = nil
			typeChanged = true
			return nil
		}
		return topo.NewError(topo.NoUpdateNeeded, agent.TabletAlias.String())
	}); err != nil {
		return err
	}

	if typeChanged {
		if err := agent.refreshTablet(ctx, "SubordinateWasRestarted"); err != nil {
			return err
		}
		agent.runHealthCheckLocked()
	}
	return nil
}

// StopReplicationAndGetStatus stops MySQL replication, and returns the
// current status.
func (agent *ActionAgent) StopReplicationAndGetStatus(ctx context.Context) (*replicationdatapb.Status, error) {
	if err := agent.lock(ctx); err != nil {
		return nil, err
	}
	defer agent.unlock()

	// get the status before we stop replication
	rs, err := agent.MysqlDaemon.SubordinateStatus()
	if err != nil {
		return nil, vterrors.Wrap(err, "before status failed")
	}
	if !rs.SubordinateIORunning && !rs.SubordinateSQLRunning {
		// no replication is running, just return what we got
		return mysql.SubordinateStatusToProto(rs), nil
	}
	if err := agent.stopSubordinateLocked(ctx); err != nil {
		return nil, vterrors.Wrap(err, "stop subordinate failed")
	}
	// now patch in the current position
	rs.Position, err = agent.MysqlDaemon.MainPosition()
	if err != nil {
		return nil, vterrors.Wrap(err, "after position failed")
	}
	return mysql.SubordinateStatusToProto(rs), nil
}

// PromoteReplica makes the current tablet the main
func (agent *ActionAgent) PromoteReplica(ctx context.Context) (string, error) {
	if err := agent.lock(ctx); err != nil {
		return "", err
	}
	defer agent.unlock()

	pos, err := agent.MysqlDaemon.Promote(agent.hookExtraEnv())
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if err := agent.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
		return "", err
	}

	// Set the server read-write
	startTime := time.Now()
	if _, err := topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topodatapb.TabletType_MASTER, logutil.TimeToProto(startTime)); err != nil {
		return "", err
	}

	// We call SetReadOnly only after the topo has been updated to avoid
	// situations where two tablets are main at the DB level but not at the vitess level
	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return "", err
	}

	// We only update agent's mainTermStartTime if we were able to update the topo.
	// This ensures that in case of a failure, we are never in a situation where the
	// tablet's timestamp is ahead of the topo's timestamp.
	agent.setMainTermStartTime(startTime)

	if err := agent.refreshTablet(ctx, "PromoteReplica"); err != nil {
		return "", err
	}

	return mysql.EncodePosition(pos), nil
}

// PromoteSubordinate makes the current tablet the main
// Deprecated
func (agent *ActionAgent) PromoteSubordinate(ctx context.Context) (string, error) {
	if err := agent.lock(ctx); err != nil {
		return "", err
	}
	defer agent.unlock()

	pos, err := agent.MysqlDaemon.Promote(agent.hookExtraEnv())
	if err != nil {
		return "", err
	}

	// If using semi-sync, we need to enable it before going read-write.
	if err := agent.fixSemiSync(topodatapb.TabletType_MASTER); err != nil {
		return "", err
	}

	// Set the server read-write
	startTime := time.Now()
	if err := agent.MysqlDaemon.SetReadOnly(false); err != nil {
		return "", err
	}

	if _, err := topotools.ChangeType(ctx, agent.TopoServer, agent.TabletAlias, topodatapb.TabletType_MASTER, logutil.TimeToProto(startTime)); err != nil {
		return "", err
	}

	// We only update agent's mainTermStartTime if we were able to update the topo.
	// This ensures that in case of a failure, we are never in a situation where the
	// tablet's timestamp is ahead of the topo's timestamp.
	agent.setMainTermStartTime(startTime)

	if err := agent.refreshTablet(ctx, "PromoteSubordinate"); err != nil {
		return "", err
	}

	return mysql.EncodePosition(pos), nil
}

func isMainEligible(tabletType topodatapb.TabletType) bool {
	switch tabletType {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA:
		return true
	}

	return false
}

func (agent *ActionAgent) fixSemiSync(tabletType topodatapb.TabletType) error {
	if !*enableSemiSync {
		// Semi-sync handling is not enabled.
		return nil
	}

	// Only enable if we're eligible for becoming main (REPLICA type).
	// Ineligible subordinates (RDONLY) shouldn't ACK because we'll never promote them.
	if !isMainEligible(tabletType) {
		return agent.MysqlDaemon.SetSemiSyncEnabled(false, false)
	}

	// Always enable subordinate-side since it doesn't hurt to keep it on for a main.
	// The main-side needs to be off for a subordinate, or else it will get stuck.
	return agent.MysqlDaemon.SetSemiSyncEnabled(tabletType == topodatapb.TabletType_MASTER, true)
}

func (agent *ActionAgent) fixSemiSyncAndReplication(tabletType topodatapb.TabletType) error {
	if !*enableSemiSync {
		// Semi-sync handling is not enabled.
		return nil
	}

	if tabletType == topodatapb.TabletType_MASTER {
		// Main is special. It is always handled at the
		// right time by the reparent operations, it doesn't
		// need to be fixed.
		return nil
	}

	if err := agent.fixSemiSync(tabletType); err != nil {
		return vterrors.Wrapf(err, "failed to fixSemiSync(%v)", tabletType)
	}

	// If replication is running, but the status is wrong,
	// we should restart replication. First, let's make sure
	// replication is running.
	status, err := agent.MysqlDaemon.SubordinateStatus()
	if err != nil {
		// Replication is not configured, nothing to do.
		return nil
	}
	if !status.SubordinateIORunning {
		// IO thread is not running, nothing to do.
		return nil
	}

	shouldAck := isMainEligible(tabletType)
	acking, err := agent.MysqlDaemon.SemiSyncSubordinateStatus()
	if err != nil {
		return vterrors.Wrap(err, "failed to get SemiSyncSubordinateStatus")
	}
	if shouldAck == acking {
		return nil
	}

	// We need to restart replication
	log.Infof("Restarting replication for semi-sync flag change to take effect from %v to %v", acking, shouldAck)
	if err := agent.MysqlDaemon.StopSubordinate(agent.hookExtraEnv()); err != nil {
		return vterrors.Wrap(err, "failed to StopSubordinate")
	}
	if err := agent.MysqlDaemon.StartSubordinate(agent.hookExtraEnv()); err != nil {
		return vterrors.Wrap(err, "failed to StartSubordinate")
	}
	return nil
}

func (agent *ActionAgent) handleRelayLogError(err error) error {
	// attempt to fix this error:
	// Subordinate failed to initialize relay log info structure from the repository (errno 1872) (sqlstate HY000) during query: START SLAVE
	// see https://bugs.mysql.com/bug.php?id=83713 or https://github.com/vitessio/vitess/issues/5067
	if strings.Contains(err.Error(), "Subordinate failed to initialize relay log info structure from the repository") {
		// Stop, reset and start subordinate again to resolve this error
		if err := agent.MysqlDaemon.RestartSubordinate(agent.hookExtraEnv()); err != nil {
			return err
		}
		return nil
	}
	return err
}
