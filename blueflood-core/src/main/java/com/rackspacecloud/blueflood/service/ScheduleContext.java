/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

// keeps track of dirty slots in memory. Operations must be threadsafe.
// todo: explore using ReadWrite locks (might not make a difference).

/**
 * Each node is responsible for sharded slots (time ranges) of rollups.  This class keeps track of the execution of
 * those rollups and the states they are in.  
 * 
 * When syncrhonizing multiple collections, do it in this order: scheduled, running, updatemap. 
 */
public class ScheduleContext implements IngestionContext {
    private static final Logger log = LoggerFactory.getLogger(ScheduleContext.class);

    private final ShardStateManager shardStateManager;
    private long scheduleTime = 0L;
    
    // these shards have been scheduled in the last 10 minutes.
    private final Cache<Integer, Long> recentlyScheduledShards = CacheBuilder.newBuilder()
            .maximumSize(Constants.NUMBER_OF_SHARDS)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    // state
    //

    // these are all the slots that are scheduled to run in no particular order. the collection is synchronized to 
    // control updates.
    private final Set<String> scheduledSlots = new HashSet<String>(); // Collections.synchronizedSet(new HashSet<String>());
    
    // same information as scheduledSlots, but order is preserved.  The ordered property is only needed for getting the
    // the next scheduled slot, but most operations are concerned with if a slot is scheduled or not.  When you update
    // one, you must update the other.
    private final List<String> orderedScheduledSlots = new ArrayList<String>();
    
    // slots that are running are not scheduled.
    private final Map<String, Long> runningSlots = new HashMap<String, Long>();

    // shard lock manager
    private ShardLockManager lockManager;

    public ScheduleContext(long currentTimeMillis, Collection<Integer> managedShards) {
        this.scheduleTime = currentTimeMillis;
        this.shardStateManager = new ShardStateManager(managedShards, asMillisecondsSinceEpochTicker());
        this.lockManager = new NoOpShardLockManager();
    }

    public ScheduleContext(long currentTimeMillis, Collection<Integer> managedShards, String zookeeperCluster) {
        this(currentTimeMillis, managedShards);
        this.lockManager = new ZKBasedShardLockManager(zookeeperCluster, new HashSet<Integer>(shardStateManager.getManagedShards()));
    }

    public void setCurrentTimeMillis(long millis){ scheduleTime = millis; }
    public long getCurrentTimeMillis() { return scheduleTime; }

    public ShardStateManager getShardStateManager() {
        return this.shardStateManager;
    }
    
    // marks slots dirty. -- This is ONLY called on a subset of host environments where Blueflood runs
    // -- it runs on scribe nodes, not dcass nodes
    public void update(long millis, int shard) {
        // there are two update paths. for managed shards, we must guard the scheduled and running collections.  but
        // for unmanaged shards, we just let the update happen uncontested.
        if (log.isTraceEnabled())
            log.trace("Updating {} to {}", shard, millis);
        boolean isManaged = shardStateManager.contains(shard);
        for (Granularity g : Granularity.rollupGranularities()) {
            ShardStateManager.SlotStateManager slotStateManager = shardStateManager.getSlotStateManager(shard, g);
            int slot = g.slot(millis);

            if (isManaged) {
                synchronized (scheduledSlots) { //write
                    if (scheduledSlots.remove(g.formatLocatorKey(slot, shard)) && log.isDebugEnabled()) {
                        log.debug("descheduled " + g.formatLocatorKey(slot, shard));// don't worry about orderedScheduledSlots
                    }
                }
            }
            slotStateManager.createOrUpdateForSlotAndMillisecond(slot, millis);
        }
    }

    // iterates over all active slots, scheduling those that haven't been updated in maxAgeMillis.
    // only one thread should be calling in this puppy.
    void scheduleSlotsOlderThan(long maxAgeMillis) {
        long now = scheduleTime;
        ArrayList<Integer> shardKeys = new ArrayList<Integer>(shardStateManager.getManagedShards());
        Collections.shuffle(shardKeys);

        for (int shard : shardKeys) {
            for (Granularity g : Granularity.rollupGranularities()) {
                // sync on map since we do not want anything added to or taken from it while we iterate.
                synchronized (scheduledSlots) { // read
                    synchronized (runningSlots) { // read
                        List<Integer> slotsToWorkOn = shardStateManager.getSlotStateManager(shard, g).getSlotsOlderThan(now, maxAgeMillis);
                        if (slotsToWorkOn.size() == 0) {
                            continue;
                        }
                        if (!canWorkOnShard(shard)) {
                            continue;
                        }

                        for (Integer slot : slotsToWorkOn) {
                            if (areChildKeysOrSelfKeyScheduledOrRunning(shard, g, slot)) {
                                continue;
                            }
                            String key = g.formatLocatorKey(slot, shard);
                            scheduledSlots.add(key);
                            orderedScheduledSlots.add(key);
                            recentlyScheduledShards.put(shard, scheduleTime);
                        }
                    }
                }
            }
        }
    }

    private boolean areChildKeysOrSelfKeyScheduledOrRunning(int shard, Granularity g, int slot) {
        // if any ineligible (children and self) keys are running or scheduled to run, we shouldn't work on this.
        Collection<String> ineligibleKeys = shardStateManager.getSlotStateManager(shard, g).getChildAndSelfKeysForSlot(slot);

        // if any ineligible keys are running or scheduled to run, do not schedule this key.
        for (String badKey : ineligibleKeys)
            if (runningSlots.keySet().contains(badKey) || scheduledSlots.contains(badKey))
                return true;

        return false;
    }

    private boolean canWorkOnShard(int shard) {
        boolean canWork = lockManager.canWork(shard);
        if (!canWork) {
            if (log.isTraceEnabled())
                log.trace("Skipping shard " + shard + " as lock could not be acquired");
        }
        return canWork;
    }
    
    // returns the next scheduled key. It has a few side effects: 1) it resets update tracking for that slot, 2) it adds
    // the key to the set of running rollups.
    String getNextScheduled() {
        synchronized (scheduledSlots) {
            if (scheduledSlots.size() == 0)
                return null;
            synchronized (runningSlots) {
                String key = orderedScheduledSlots.remove(0);
                int slot = Granularity.slotFromKey(key);
                Granularity gran = Granularity.granularityFromKey(key);
                int shard = Granularity.shardFromKey(key);
                // notice how we change the state, but the timestamp remained the same. this is important.  When the
                // state is evaluated (i.e., in Reader.getShardState()) we need to realize that when timstamps are the
                // same (this will happen), that a remove always wins during the coalesce.
                scheduledSlots.remove(key);

                UpdateStamp stamp = shardStateManager.getSlotStateManager(shard, gran).getAndSetState(slot, UpdateStamp.State.Running);
                runningSlots.put(key, stamp.getTimestamp());
                return key;
            }
        }
    }
    
    void pushBackToScheduled(String key) {
        synchronized (scheduledSlots) {
            synchronized (runningSlots) {
                int slot = Granularity.slotFromKey(key);
                Granularity gran = Granularity.granularityFromKey(key);
                int shard = Granularity.shardFromKey(key);
                // no need to set dirty/clean here.
                shardStateManager.getSlotStateManager(shard, gran).getAndSetState(slot, UpdateStamp.State.Active);
                scheduledSlots.add(key);
                orderedScheduledSlots.add(0, key);
            }
        }
    }
    
    // remove rom the list of running slots.
    void clearFromRunning(String slotKey) {
        int shard = Granularity.shardFromKey(slotKey);
        synchronized (runningSlots) {
            runningSlots.remove(slotKey);
            int slot = Granularity.slotFromKey(slotKey);
            Granularity gran = Granularity.granularityFromKey(slotKey);

            UpdateStamp stamp = shardStateManager.getUpdateStamp(shard, gran, slot);
            shardStateManager.setAllCoarserSlotsDirtyForChildSlot(shard, gran, slot, stamp.getTimestamp());
            // Update the stamp to Rolled state if and only if the current state is running.
            // If the current state is active, it means we received a delayed write which toggled the status to Active.
            if (stamp.getState() == UpdateStamp.State.Running) {
                stamp.setState(UpdateStamp.State.Rolled);
                stamp.setTimestamp(getCurrentTimeMillis());
                stamp.setDirty(true);
            }
        }
    }

    // true if anything is scheduled.
    boolean hasScheduled() {
        return getScheduledCount() > 0;
    }
    
    // returns the number of scheduled rollups.
    int getScheduledCount() {
        synchronized (scheduledSlots) {
            return scheduledSlots.size();
        }
    }

    public Map<Integer, UpdateStamp> getSlotStamps(Granularity gran, int shard) {
        return shardStateManager.getSlotStateManager(shard, gran).getSlotStamps();
    }

    // precondition: shard is unmanaged.
    void addShard(int shard) {
        shardStateManager.add(shard);
        lockManager.addShard(shard);    
    }
    
    // precondition: shard is managed.
    void removeShard(int shard) {
        shardStateManager.remove(shard);
        lockManager.removeShard(shard);
    }

    Set<Integer> getRecentlyScheduledShards() {
        // Collections.unmodifiableSet(...) upsets JMX.
        return new TreeSet<Integer>(recentlyScheduledShards.asMap().keySet());
    }

    // Normal ticker behavior is to return nanoseconds elapsed since VM started.
    // This returns milliseconds since the epoch based upon ScheduleContext's internal representation of time (scheduleTime).
    public Ticker asMillisecondsSinceEpochTicker() {
        return new Ticker() {
            @Override
            public long read() {
                return ScheduleContext.this.getCurrentTimeMillis();
            }
        };
    }
}
