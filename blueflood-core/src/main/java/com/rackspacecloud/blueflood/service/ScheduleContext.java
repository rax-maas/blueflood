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

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * <pr>
 * Tracks the scheduling & the executions of the slot checks.
 * </pr>
 *
 * NOTE: Operations on this class must be threadsafe.
 * NOTE: When synchronizing multiple collections, do it in this order: scheduled -> running.
 * NOTE: Explore using ReadWrite locks (might not make a difference).
 */
public class ScheduleContext implements IngestionContext, SlotCheckScheduler, ShardedService {
    private static final Logger log = LoggerFactory.getLogger(ScheduleContext.class);
    private final ShardStateManager shardStateManager;
    private volatile long scheduleTime = 0L;

    /**
     * These shards have been scheduled in the last 10 minutes.
     */
    private final Cache<Integer, Long> recentlyScheduledShards = CacheBuilder.newBuilder()
            .maximumSize(Constants.NUMBER_OF_SHARDS)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    /**
     * Incremented when a given slot check is unscheduled due to the shard ownership change.
     */
    private final Meter shardOwnershipChanged = Metrics.meter(ScheduleContext.class, "Shard Change Before Running");

    /**
     * Slots scheduled to run in no particular order.
     * The collection is synchronized to control updates.
     */
    private final Set<SlotKey> scheduledSlots = new HashSet<SlotKey>();

    /**
     * Order-preserved {@link #scheduledSlots}.
     * The ordered property is only needed for getting the next scheduled slot, but most operations are concerned with
     * if a slot is scheduled or not. when you update {@link #scheduledSlots}, {@link #orderedScheduledSlots} must be
     * updated also.
     */
    private final List<SlotKey> orderedScheduledSlots = new ArrayList<SlotKey>();

    /**
     * Slots that are running and not scheduled. Maps to the started timestamp.
     */
    private final Map<SlotKey, Long> runningSlots = new HashMap<SlotKey, Long>();

    /**
     * Shard lock manager. Determines whether the given shard should be worked on or not.
     */
    private final ShardLockManager lockManager;

    /**
     * Constructor that doesn't use zookeeper shard manager.
     * @param currentTimeMillis current time, in ms.
     * @param managedShards initially managed shards.
     */
    public ScheduleContext(long currentTimeMillis, Collection<Integer> managedShards) {
        this.scheduleTime = currentTimeMillis;
        this.shardStateManager = new ShardStateManager(managedShards, asMillisecondsSinceEpochTicker());
        this.lockManager = new NoOpShardLockManager();
    }

    /**
     * Constructor with the zookeeper shard manager.
     * @param currentTimeMillis current time, in ms.
     * @param managedShards initially managed shards.
     * @param zookeeperCluster zookeeper server to connect to.
     */
    public ScheduleContext(long currentTimeMillis, Collection<Integer> managedShards, String zookeeperCluster) {
        this.scheduleTime = currentTimeMillis;
        this.shardStateManager = new ShardStateManager(managedShards, asMillisecondsSinceEpochTicker());
        ZKBasedShardLockManager lockManager = new ZKBasedShardLockManager(zookeeperCluster, new HashSet<Integer>(shardStateManager.getManagedShards()));
        lockManager.init(new TimeValue(5, TimeUnit.SECONDS));
        this.lockManager = lockManager;
    }

    public void setCurrentTimeMillis(long millis){ scheduleTime = millis; }
    public long getCurrentTimeMillis() { return scheduleTime; }

    public ShardStateManager getShardStateManager() {
        return this.shardStateManager;
    }

    /**
     * {@inheritDoc}
     */
    public void update(long millis, int shard) {
        // there are two update paths. for managed shards, we must guard the scheduled and running collections.  but
        // for unmanaged shards, we just let the update happen uncontested.
        if (log.isTraceEnabled()) {
            log.trace("Updating {} to {}", shard, millis);
        }
        boolean isManaged = shardStateManager.contains(shard);
        for (Granularity g : Granularity.rollupGranularities()) {
            ShardStateManager.SlotStateManager slotStateManager = shardStateManager.getSlotStateManager(shard, g);
            int slot = g.slot(millis);

            if (isManaged) {
                synchronized (scheduledSlots) { //put
                    SlotKey key = SlotKey.of(g, slot, shard);
                    if (scheduledSlots.remove(key) && log.isDebugEnabled()) {
                        log.debug("descheduled {}.", key);// don't worry about orderedScheduledSlots
                    }
                }
            }
            slotStateManager.createOrUpdateForSlotAndMillisecond(slot, millis);
        }
    }

    /**
     * Iterate over all active slots, scheduling those that haven't been updated for a while.
     * @param minAgeMs Minimum elapsed milliseconds since the last update to be eligible for queuing.
     */
    void scheduleSlotsOlderThan(long minAgeMs) {
        long now = scheduleTime;
        ArrayList<Integer> shardKeys = new ArrayList<Integer>(shardStateManager.getManagedShards());
        Collections.shuffle(shardKeys);

        for (int shard : shardKeys) {
            for (Granularity g : Granularity.rollupGranularities()) {
                // sync on map since we do not want anything added to or taken from it while we iterate.
                synchronized (scheduledSlots) { // read
                    synchronized (runningSlots) { // read
                        List<Integer> slotsToWorkOn = shardStateManager.getSlotStateManager(shard, g).getSlotsOlderThan(now, minAgeMs);
                        if (slotsToWorkOn.size() == 0) {
                            continue;
                        }
                        if (!canWorkOnShard(shard)) {
                            continue;
                        }

                        for (Integer slot : slotsToWorkOn) {
                            SlotKey slotKey = SlotKey.of(g, slot, shard);
                            if (areChildKeysOrSelfKeyScheduledOrRunning(slotKey)) {
                                continue;
                            }
                            SlotKey key = SlotKey.of(g, slot, shard);
                            scheduledSlots.add(key);
                            orderedScheduledSlots.add(key);
                            recentlyScheduledShards.put(shard, scheduleTime);
                        }
                    }
                }
            }
        }
    }

    private boolean areChildKeysOrSelfKeyScheduledOrRunning(SlotKey slotKey) {
        // if any ineligible (children and self) keys are running or scheduled to run, we shouldn't work on this.
        Collection<SlotKey> ineligibleKeys = slotKey.getChildrenKeys();

        if (runningSlots.keySet().contains(slotKey) || scheduledSlots.contains(slotKey)) {
            return true;
        }

        // if any ineligible keys are running or scheduled to run, do not schedule this key.
        for (SlotKey childrenKey : ineligibleKeys) {
            if (runningSlots.keySet().contains(childrenKey) || scheduledSlots.contains(childrenKey)) {
                return true;
            }
        }

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

    /**
     * {@inheritDoc}
     */
    @Override public SlotKey getNextScheduled() {
        synchronized (scheduledSlots) {
            if (scheduledSlots.size() == 0)
                return null;
            synchronized (runningSlots) {
                SlotKey key = orderedScheduledSlots.remove(0);
                int slot = key.getSlot();
                Granularity gran = key.getGranularity();
                int shard = key.getShard();
                // notice how we change the state, but the timestamp remained the same. this is important.  When the
                // state is evaluated (i.e., in Reader.getShardState()) we need to realize that when timstamps are the
                // same (this will happen), that a remove always wins during the coalesce.
                scheduledSlots.remove(key);

                if (canWorkOnShard(shard)) {
                    UpdateStamp stamp = shardStateManager.getSlotStateManager(shard, gran).getAndSetState(slot, UpdateStamp.State.Running);
                    runningSlots.put(key, stamp.getTimestamp());
                    return key;
                } else {
                    shardOwnershipChanged.mark();
                    return null;
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reschedule(SlotKey slotKey, boolean rescheduleImmediately) {
        synchronized (scheduledSlots) {
            synchronized (runningSlots) {
                runningSlots.remove(slotKey);
                int slot = slotKey.getSlot();
                int shard = slotKey.getShard();
                // no need to set dirty/clean here, because the slot check will be re-scheduled.
                shardStateManager.getSlotStateManager(shard, slotKey.getGranularity()).getAndSetState(slot, UpdateStamp.State.Active);
                scheduledSlots.add(slotKey);
                if (rescheduleImmediately) {
                    orderedScheduledSlots.add(0, slotKey);
                } else {
                    orderedScheduledSlots.add(slotKey);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void markFinished(SlotKey slotKey) {
        synchronized (runningSlots) {
            runningSlots.remove(slotKey);
            UpdateStamp stamp = shardStateManager.getUpdateStamp(slotKey);
            shardStateManager.setAllCoarserSlotsDirtyForSlot(slotKey);
            // Update the stamp to Rolled state if and only if the current state is running.
            // If the current state is active, it means we received a delayed put which toggled the status to Active.
            if (stamp.getState() == UpdateStamp.State.Running) {
                stamp.setState(UpdateStamp.State.Rolled);
                // Note: Rollup state will be updated to the last ACTIVE timestamp which caused rollup process to kick in.
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
    @Override public void addShard(int shard) {
        shardStateManager.addShard(shard);
        lockManager.addShard(shard);    
    }
    
    // precondition: shard is managed.
    @Override public void removeShard(int shard) {
        shardStateManager.removeShard(shard);
        lockManager.removeShard(shard);
    }

    Set<Integer> getRecentlyScheduledShards() {
        // Collections.unmodifiableSet(...) upsets JMX.
        return new TreeSet<Integer>(recentlyScheduledShards.asMap().keySet());
    }

    // Normal ticker behavior is to return nanoseconds elapsed since VM started.
    // This returns milliseconds since the epoch based upon ScheduleContext's internal representation of time (scheduleTime).
    private Ticker asMillisecondsSinceEpochTicker() {
        return new Ticker() {
            @Override
            public long read() {
                return ScheduleContext.this.getCurrentTimeMillis();
            }
        };
    }

    @VisibleForTesting int getRunningCount() {
        synchronized (runningSlots) {
            return runningSlots.size();
        }
    }
}
