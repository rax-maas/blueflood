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
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
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
 *
 * The {@code ScheduleContext} class coordinates access to the states of slots.
 * It mediates between the rollup service, the ingestion service, and the
 * database.
 * <p>
 *
 * The class's chief purpose is to coordinate access to slots' states and the
 * list of slots that need to be re-rolled between {@link RollupService},
 * {@link IngestionService}, and the database. There's several other classes
 * and components involved, but those are the most important.
 * <p>
 *
 * When ingestion services receive incoming metrics, they will change the
 * associated slot's state to
 * {@link com.rackspacecloud.blueflood.service.UpdateStamp.State#Active Active}
 * (needs to be re-rolled) by calling {@link #update(long, int)}. That state
 * information will be persisted to the database by the
 * {@link ShardStatePusher} class. On the rollup nodes, the
 * {@link ShardStatePuller} will read the state info into memory by calling
 * {@link com.rackspacecloud.blueflood.service.ShardStateManager.SlotStateManager#updateSlotOnRead(SlotState)}.
 * The rollup service will then identity slots that need to be re-rolled (by
 * calling {@link #scheduleEligibleSlots(long, long, long)}), pick a slot to rollup and
 * mark it as
 * {@link com.rackspacecloud.blueflood.service.UpdateStamp.State#Running Running}
 * (via {@link #getNextScheduled()} ), do the rollup, and then mark the slot as
 * {@link com.rackspacecloud.blueflood.service.UpdateStamp.State#Rolled Rolled}
 * (via {@link #clearFromRunning(SlotKey)}).
 * <p>
 *
 * If doing the rollup fails for some reason, the rollup service will call
 * {@link #pushBackToScheduled(SlotKey, boolean)}, to return the slot to the
 * queue of slots to be rolled.
 * <p>
 *
 * There are additional methods for managing shards ({@link #addShard(int)} and
 * {@link #removeShard(int)}) which don't appear to be used.
 * <p>
 *
 * There are also methods which provide information about the state of things
 * (e.g. {@link #getScheduledCount()},
 * {@link #getSlotStamps(Granularity, int)},
 * {@link #getRecentlyScheduledShards()},
 * {@link #getMetricsState(int, String, int)}) which either aren't used, are
 * only used for testing, or are used to provide information to external system
 * via JMX or yammer metrics.
 * <p>
 *
 *
 * Previous comments, left in case we need them:
 * Keeps track of dirty slots in memory. Operations must be threadsafe.
 *
 * todo: explore using ReadWrite locks (might not make a difference).
 *
 * Each node is responsible for sharded slots (time ranges) of rollups. This
 * class keeps track of the execution of those rollups and the states they are
 * in.
 *
 * When synchronizing multiple collections, do it in this order: scheduled -> running.
 */
public class ScheduleContext implements IngestionContext, ScheduleContextMBean {
    private static final Logger log = LoggerFactory.getLogger(ScheduleContext.class);
    private final Timer markSlotDirtyTimer = Metrics.timer(ScheduleContext.class, "Slot Mark Dirty Duration");

    private final ShardStateManager shardStateManager;
    private transient long scheduleTime = 0L;
    
    /** these shards have been scheduled in the last 10 minutes. */
    private final Cache<Integer, Long> recentlyScheduledShards = CacheBuilder.newBuilder()
            .maximumSize(Constants.NUMBER_OF_SHARDS)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    // state
    //
    private final Meter shardOwnershipChanged = Metrics.meter(ScheduleContext.class, "Shard Change Before Running");

    /**
     * these are all the slots that are scheduled to run in no particular order.
     * the collection is synchronized to control updates.
     */
    private final Set<SlotKey> scheduledSlots = new HashSet<SlotKey>();

    /**
     * same information as {@link #scheduledSlots}, but order is preserved. The
     * ordered property is only needed for getting the the next scheduled slot,
     * but most operations are concerned with if a slot is scheduled or not.
     * When you update one, you must update the other.
     */
    private final List<SlotKey> orderedScheduledSlots = new ArrayList<SlotKey>();
    
    /** slots that are running are not scheduled. */
    private final Map<SlotKey, Long> runningSlots = new HashMap<SlotKey, Long>();

    /** shard lock manager */
    private final ShardLockManager lockManager;

    private final Clock clock;

    public ScheduleContext(long currentTimeMillis, Collection<Integer> managedShards, Clock clock) {
        this.scheduleTime = currentTimeMillis;
        this.shardStateManager = new ShardStateManager(managedShards, asMillisecondsSinceEpochTicker(), clock);
        this.lockManager = new NoOpShardLockManager();
        this.clock = clock;
        registerMBean();
    }

    public ScheduleContext(long currentTimeMillis, Collection<Integer> managedShards, String zookeeperCluster) {
        this.scheduleTime = currentTimeMillis;
        this.shardStateManager = new ShardStateManager(managedShards, asMillisecondsSinceEpochTicker());
        ZKShardLockManager lockManager = new ZKShardLockManager(zookeeperCluster, new HashSet<Integer>(shardStateManager.getManagedShards()));
        lockManager.init(new TimeValue(5, TimeUnit.SECONDS));
        this.lockManager = lockManager;
        this.clock = new DefaultClockImpl();
        registerMBean();
    }

    public ScheduleContext(long currentTimeMillis, Collection<Integer> managedShards) {
        this(currentTimeMillis, managedShards, new DefaultClockImpl());
    }

    @VisibleForTesting
    public ScheduleContext(long currentTimeMillis,
                           Collection<Integer> managedShards,
                           Clock clock,
                           ShardStateManager shardStateManager,
                           ShardLockManager shardLockManager) {
        this.scheduleTime = currentTimeMillis;
        this.shardStateManager = shardStateManager;
        this.lockManager = shardLockManager;
        this.clock = clock;
        registerMBean();
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
        // there are two update paths. for managed shards, we must guard the
        // scheduled and running collections. but for unmanaged shards, we just
        // let the update happen uncontested.
        final Timer.Context dirtyTimerCtx = markSlotDirtyTimer.time();
        try {
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
                            // don't worry about orderedScheduledSlots
                            log.debug("descheduled {}.", key);
                        }
                    }
                }
                slotStateManager.createOrUpdateForSlotAndMillisecond(slot, millis);
            }
        } finally {
            dirtyTimerCtx.stop();
        }
    }

    /**
     * Loop through all slots that are eligible for rollup, at all
     * granularities, in all managed shards. If any are found that are not
     * already running or scheduled, then add them to the queue of scheduled
     * slots.
     *
     * Note that {@code maxAgeMillis}, {@code rollupDelayForMetricsWithShortDelay}
     * {@code rollupWaitForMetricsWithLongDelay} are age values, not a timestamp.
     *
     */
    // only one thread should be calling in this puppy.
    void scheduleEligibleSlots(long maxAgeMillis, long rollupDelayForMetricsWithShortDelay, long rollupWaitForMetricsWithLongDelay) {
        long now = scheduleTime;
        ArrayList<Integer> shardKeys = new ArrayList<Integer>(shardStateManager.getManagedShards());
        Collections.shuffle(shardKeys);

        for (int shard : shardKeys) {
            for (Granularity g : Granularity.rollupGranularities()) {
                // sync on map since we do not want anything added to or taken from it while we iterate.
                synchronized (scheduledSlots) { // read
                    synchronized (runningSlots) { // read

                        List<Integer> slotsToWorkOn = shardStateManager.getSlotStateManager(shard, g)
                                .getSlotsEligibleForRollup(now, maxAgeMillis, rollupDelayForMetricsWithShortDelay, rollupWaitForMetricsWithLongDelay);

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

    boolean isReroll(SlotKey slotKey) {
        return shardStateManager.getSlotStateManager(slotKey.getShard(), slotKey.getGranularity())
                .isReroll(slotKey.getSlot(), scheduleTime);
    }

    boolean areChildKeysOrSelfKeyScheduledOrRunning(SlotKey slotKey) {
        // if any ineligible (children and self) keys are running or scheduled to run, we shouldn't work on this.
        Collection<SlotKey> ineligibleKeys = slotKey.getChildrenKeys();

        if (runningSlots.keySet().contains(slotKey)) {
            return true;
        }
        if (scheduledSlots.contains(slotKey)) {
            return true;
        }

        // if any ineligible keys are running or scheduled to run, do not schedule this key.
        for (SlotKey childrenKey : ineligibleKeys) {
            if (runningSlots.keySet().contains(childrenKey)) {
                return true;
            }
            if (scheduledSlots.contains(childrenKey)) {
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
     * Returns the next scheduled key. It has a few side effects:
     *      1) it resets update tracking for that slot
     *      2) it adds the key to the set of running rollups.
     *
     * @return
     */
    @VisibleForTesting
    SlotKey getNextScheduled() {
        synchronized (scheduledSlots) {
            if (scheduledSlots.size() == 0)
                return null;
            synchronized (runningSlots) {
                SlotKey key = orderedScheduledSlots.remove(0);
                int slot = key.getSlot();
                Granularity gran = key.getGranularity();
                int shard = key.getShard();

                // notice how we change the state, but the timestamp remained
                // the same. this is important.  When the state is evaluated
                // (i.e., in Reader.getShardState()) we need to realize that
                // when timestamps are the same (this will happen), that a
                // remove always wins during the coalesce.
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
     * Take the given slot out of the running group, and put it back into the
     * scheduled group. If {@code rescheduleImmediately} is true, the slot will
     * be the next slot returned by a call to {@link #getNextScheduled()}. If
     * {@code rescheduleImmediately} is false, then the given slot will go to
     * the end of the line, as when it was first scheduled by
     * {@link #scheduleEligibleSlots(long, long, long)}.
     *
     * @param key
     * @param rescheduleImmediately
     */
    void pushBackToScheduled(SlotKey key, boolean rescheduleImmediately) {
        synchronized (scheduledSlots) {
            synchronized (runningSlots) {
                int slot = key.getSlot();
                Granularity gran = key.getGranularity();
                int shard = key.getShard();
                // no need to set dirty/clean here.
                shardStateManager.getSlotStateManager(shard, gran).getAndSetState(slot, UpdateStamp.State.Active);
                scheduledSlots.add(key);
                log.debug("pushBackToScheduled -> added to scheduledSlots: " + key + " size:" + scheduledSlots.size());
                if (rescheduleImmediately) {
                    orderedScheduledSlots.add(0, key);
                } else {
                    orderedScheduledSlots.add(key);
                }
            }
        }
    }

    /**
     * Remove the given slot from the running group after it has been
     * successfully re-rolled.
     *
     * @param slotKey
     */
    void clearFromRunning(SlotKey slotKey) {
        synchronized (runningSlots) {
            runningSlots.remove(slotKey);
            UpdateStamp stamp = shardStateManager.getUpdateStamp(slotKey);
            shardStateManager.setAllCoarserSlotsDirtyForSlot(slotKey);

            //When state gets set to "X", before it got persisted, it might get scheduled for rollup
            //again, if we get delayed metrics. To prevent this we temporarily set last rollup time with current
            //time. This value wont get persisted.
            long currentTimeInMillis = clock.now().getMillis();
            stamp.setLastRollupTimestamp(currentTimeInMillis);
            log.debug("SlotKey {} is marked in memory with last rollup time as {}", slotKey, currentTimeInMillis);

            // Update the stamp to Rolled state if and only if the current state
            // is running. If the current state is active, it means we received
            // a delayed put which toggled the status to Active.
            if (stamp.getState() == UpdateStamp.State.Running) {
                stamp.setState(UpdateStamp.State.Rolled);
                // Note: Rollup state will be updated to the last ACTIVE
                // timestamp which caused rollup process to kick in.
                stamp.setDirty(true);
            }
        }
    }

    /**
     * true if anything is scheduled.
     */
    boolean hasScheduled() {
        return getScheduledCount() > 0;
    }

    /**
     * returns the number of scheduled rollups.
     */
    int getScheduledCount() {
        synchronized (scheduledSlots) {
            return scheduledSlots.size();
        }
    }

    /**
     * returns the number of currently running rollups.
     */
    @VisibleForTesting
    int getRunningCount() {
        synchronized (runningSlots) {
            return runningSlots.size();
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

    /**
     * Normal {@link com.google.common.base.Ticker Ticker} behavior is to
     * return nanoseconds elapsed since VM started. This returns milliseconds
     * since the epoch based upon {@code ScheduleContext}'s internal
     * representation of time ({@link #scheduleTime}).
     *
     * @return an anonymous Ticker object
     */
    //
    public Ticker asMillisecondsSinceEpochTicker() {
        return new Ticker() {
            @Override
            public long read() {
                return ScheduleContext.this.getCurrentTimeMillis();
            }
        };
    }

    @Override
    public Collection<String> getMetricsState(int shard, String gran, int slot) {
        final List<String> results = new ArrayList<String>();
        Granularity granularity = Granularity.fromString(gran);

        if (granularity == null)
            return results;

        final Map<Integer, UpdateStamp> stateTimestamps = this.getSlotStamps(granularity, shard);

        if (stateTimestamps == null)
            return results;

        final UpdateStamp stamp = stateTimestamps.get(slot);
        if (stamp != null) {
            results.add(new SlotState(granularity, slot, stamp.getState()).withTimestamp(stamp.getTimestamp()).toString());
        }

        return results;
    }

    private boolean isMbeanRegistered = false;
    private synchronized void registerMBean() {

        if (isMbeanRegistered) return;
        isMbeanRegistered = true;

        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final String name = String.format("com.rackspacecloud.blueflood.io:type=%s", ScheduleContext.class.getSimpleName());
            final ObjectName nameObj = new ObjectName(name);
            mbs.registerMBean(this, nameObj);
        } catch (Exception exc) {
            log.error("Unable to register mbean for " + ScheduleContext.class.getSimpleName(), exc);
        }
    }
}
