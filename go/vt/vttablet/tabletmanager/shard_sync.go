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
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	shardSyncRetryDelay = flag.Duration("shard_sync_retry_delay", 30*time.Second, "delay between retries of updates to keep the tablet and its shard record in sync")
)

// shardSyncLoop is a loop that tries to keep the tablet state and the
// shard record in sync.
//
// It is launched as a background goroutine in the tablet because it may need to
// initiate a tablet state change in response to an incoming watch event for the
// shard record, and it may need to continually retry updating the shard record
// if it's out of sync with the tablet state. At steady state, when the tablet
// and shard record are in sync, this goroutine goes to sleep waiting for
// something to change in either the tablet state or in the shard record.
//
// This goroutine gets woken up for shard record changes by maintaining a
// topo watch on the shard record. It gets woken up for tablet state changes by
// a notification signal from setTablet().
func (agent *ActionAgent) shardSyncLoop(ctx context.Context) {
	// Make a copy of the channels so we don't race when stopShardSync() clears them.
	agent.mutex.Lock()
	notifyChan := agent._shardSyncChan
	doneChan := agent._shardSyncDone
	agent.mutex.Unlock()

	defer close(doneChan)

	// retryChan is how we wake up after going to sleep between retries.
	// If no retry is pending, this channel will be nil, which means it's fine
	// to always select on it -- a nil channel is never ready.
	var retryChan <-chan time.Time

	// shardWatch is how we get notified when the shard record is updated.
	// We only watch the shard record while we are main.
	shardWatch := &shardWatcher{}
	defer shardWatch.stop()

	// This loop sleeps until it's notified that something may have changed.
	// Then it wakes up to check if anything needs to be synchronized.
	for {
		select {
		case <-notifyChan:
			// Something may have changed in the tablet state.
			log.Info("Change to tablet state")
		case <-retryChan:
			// It's time to retry a previous failed sync attempt.
			log.Info("Retry sync")
		case event := <-shardWatch.watchChan:
			// Something may have changed in the shard record.
			// We don't use the watch event except to know that we should
			// re-read the shard record, and to know if the watch dies.
			log.Info("Change in shard record")
			if event.Err != nil {
				// The watch failed. Stop it so we start a new one if needed.
				log.Errorf("Shard watch failed: %v", event.Err)
				shardWatch.stop()
			}
		case <-ctx.Done():
			// Our context was cancelled. Terminate the loop.
			return
		}

		// Disconnect any pending retry timer since we're already retrying for
		// another reason.
		retryChan = nil

		// Get the latest internal tablet value, representing what we think we are.
		tablet := agent.Tablet()

		switch tablet.Type {
		case topodatapb.TabletType_MASTER:
			// If we think we're main, check if we need to update the shard record.
			mainAlias, err := syncShardMain(ctx, agent.TopoServer, tablet, agent.mainTermStartTime())
			if err != nil {
				log.Errorf("Failed to sync shard record: %v", err)
				// Start retry timer and go back to sleep.
				retryChan = time.After(*shardSyncRetryDelay)
				continue
			}
			if !topoproto.TabletAliasEqual(mainAlias, tablet.Alias) {
				// Another main has taken over while we still think we're main.
				if err := agent.abortMainTerm(ctx, mainAlias); err != nil {
					log.Errorf("Failed to abort main term: %v", err)
					// Start retry timer and go back to sleep.
					retryChan = time.After(*shardSyncRetryDelay)
					continue
				}
				// We're not main anymore, so stop watching the shard record.
				shardWatch.stop()
				continue
			}

			// As long as we're main, watch the shard record so we'll be
			// notified if another main takes over.
			if shardWatch.active() {
				// We already have an active watch. Nothing to do.
				continue
			}
			if err := shardWatch.start(ctx, agent.TopoServer, tablet.Keyspace, tablet.Shard); err != nil {
				log.Errorf("Failed to start shard watch: %v", err)
				// Start retry timer and go back to sleep.
				retryChan = time.After(*shardSyncRetryDelay)
				continue
			}
		default:
			// If we're not main, stop watching the shard record,
			// so only mains contribute to global topo watch load.
			shardWatch.stop()
		}
	}
}

// syncShardMain is called when we think we're main.
// It checks that the shard record agrees, and updates it if possible.
//
// If the returned error is nil, the returned mainAlias indicates the current
// main tablet according to the shard record.
//
// If the shard record indicates a new main has taken over, this returns
// success (we successfully synchronized), but the returned mainAlias will be
// different from the input tablet.Alias.
func syncShardMain(ctx context.Context, ts *topo.Server, tablet *topodatapb.Tablet, mainTermStartTime time.Time) (mainAlias *topodatapb.TabletAlias, err error) {
	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()

	var shardInfo *topo.ShardInfo
	_, err = ts.UpdateShardFields(ctx, tablet.Keyspace, tablet.Shard, func(si *topo.ShardInfo) error {
		lastTerm := si.GetMainTermStartTime()

		// Save the ShardInfo so we can check it afterward.
		// We can't use the return value of UpdateShardFields because it might be nil.
		shardInfo = si

		// Only attempt an update if our term is more recent.
		if !mainTermStartTime.After(lastTerm) {
			return topo.NewError(topo.NoUpdateNeeded, si.ShardName())
		}

		aliasStr := topoproto.TabletAliasString(tablet.Alias)
		log.Infof("Updating shard record: main_alias=%v, main_term_start_time=%v", aliasStr, mainTermStartTime)
		si.MainAlias = tablet.Alias
		si.MainTermStartTime = logutil.TimeToProto(mainTermStartTime)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return shardInfo.MainAlias, nil
}

// abortMainTerm is called when we unexpectedly lost mainship.
//
// Under normal circumstances, we should be gracefully demoted before a new
// main appears. This function is only reached when that graceful demotion
// failed or was skipped, so we only found out we're no longer main after the
// new main started advertising itself.
//
// If active reparents are enabled, we demote our own MySQL to a replica and
// update our tablet type to REPLICA.
//
// If active reparents are disabled, we don't touch our MySQL.
// We just directly update our tablet type to REPLICA.
func (agent *ActionAgent) abortMainTerm(ctx context.Context, mainAlias *topodatapb.TabletAlias) error {
	mainAliasStr := topoproto.TabletAliasString(mainAlias)
	log.Warningf("Another tablet (%v) has won main election. Stepping down to %v.", mainAliasStr, agent.DemoteMainType)

	if *mysqlctl.DisableActiveReparents {
		// Don't touch anything at the MySQL level. Just update tablet state.
		log.Infof("Active reparents are disabled; updating tablet state only.")
		changeTypeCtx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer cancel()
		if err := agent.ChangeType(changeTypeCtx, agent.DemoteMainType); err != nil {
			return vterrors.Wrapf(err, "failed to change type to %v", agent.DemoteMainType)
		}
		return nil
	}

	// Do a full demotion to convert MySQL into a replica.
	// We do not revert on partial failure here because this code path only
	// triggers after a new main has taken over, so we are past the point of
	// no return. Instead, we should leave partial results and retry the rest
	// later.
	log.Infof("Active reparents are enabled; converting MySQL to replica.")
	demoteMainCtx, cancelDemoteMain := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancelDemoteMain()
	if _, err := agent.demoteMain(demoteMainCtx, false /* revertPartialFailure */); err != nil {
		return vterrors.Wrap(err, "failed to demote main")
	}
	setMainCtx, cancelSetMain := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancelSetMain()
	log.Infof("Attempting to reparent self to new main %v.", mainAliasStr)
	if err := agent.SetMain(setMainCtx, mainAlias, 0, "", true); err != nil {
		return vterrors.Wrap(err, "failed to reparent self to new main")
	}
	return nil
}

func (agent *ActionAgent) startShardSync() {
	// Use a buffer size of 1 so we can remember we need to check the state
	// even if the receiver is busy. We can drop any additional send attempts
	// if the buffer is full because all we care about is that the receiver will
	// be told it needs to recheck the state.
	agent.mutex.Lock()
	agent._shardSyncChan = make(chan struct{}, 1)
	agent._shardSyncDone = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	agent._shardSyncCancel = cancel
	agent.mutex.Unlock()

	// Queue up a pending notification to force the loop to run once at startup.
	agent.notifyShardSync()

	// Start the sync loop in the background.
	go agent.shardSyncLoop(ctx)
}

func (agent *ActionAgent) stopShardSync() {
	var doneChan <-chan struct{}

	agent.mutex.Lock()
	if agent._shardSyncCancel != nil {
		agent._shardSyncCancel()
		agent._shardSyncCancel = nil
		agent._shardSyncChan = nil

		doneChan = agent._shardSyncDone
		agent._shardSyncDone = nil
	}
	agent.mutex.Unlock()

	// If the shard sync loop was running, wait for it to fully stop.
	if doneChan != nil {
		<-doneChan
	}
}

func (agent *ActionAgent) notifyShardSync() {
	// If this is called before the shard sync is started, do nothing.
	agent.mutex.Lock()
	defer agent.mutex.Unlock()

	if agent._shardSyncChan == nil {
		return
	}

	// Try to send. If the channel buffer is full, it means a notification is
	// already pending, so we don't need to do anything.
	select {
	case agent._shardSyncChan <- struct{}{}:
	default:
	}
}

// setMainTermStartTime remembers the time when our term as main began.
//
// If another tablet claims to be main and offers a more recent time,
// that tablet will be trusted over us.
func (agent *ActionAgent) setMainTermStartTime(t time.Time) {
	agent.mutex.Lock()
	agent._mainTermStartTime = t
	agent._replicationDelay = 0
	agent.mutex.Unlock()

	// Notify the shard sync loop that the tablet state changed.
	agent.notifyShardSync()
}

func (agent *ActionAgent) mainTermStartTime() time.Time {
	agent.mutex.Lock()
	defer agent.mutex.Unlock()
	return agent._mainTermStartTime
}
