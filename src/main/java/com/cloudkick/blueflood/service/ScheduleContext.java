package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.inputs.handlers.ScribeHandler;
import com.cloudkick.blueflood.io.Constants;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.utils.Util;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.core.Timer;
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
    
    private static final Granularity[] GRANULARITIES = new Granularity[] {
        Granularity.MIN_5,
        Granularity.MIN_20, 
        Granularity.MIN_60, 
        Granularity.MIN_240,
        Granularity.MIN_1440
    };

    private static final Set<Integer> ALL_SHARDS = new HashSet<Integer>(Util.parseShards("ALL"));
    private final Set<Integer> managedShards;
    private long currentTimeMillis = 0L;
    
    // these shards have been scheduled in the last 10 minutes.
    private final Cache<Integer, Long> recentlyScheduledShards = CacheBuilder.newBuilder()
            .maximumSize(Constants.NUMBER_OF_SHARDS)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    // Metrics
    private final Timer applySlotStampsTimer = Metrics.newTimer(RollupService.class, "ScheduleContext Apply Slot Stamps Timer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Timer rollupExecuteTimer = Metrics.newTimer(RollupService.class, "Rollup Execution Timer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Histogram rollupWaitHist = Metrics.newHistogram(RollupService.class, "Rollup Wait Histogram", true);
    private final Timer rollupLocatorExecuteTimer = Metrics.newTimer(RollupService.class, "Locate and Schedule Rollups for Slot", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Histogram timeSinceUpdate = Metrics.newHistogram(RollupService.class, "Shard Slot Time Elapsed scheduleSlotsOlderThan", true);
    private final Meter updateStampMeter = Metrics.newMeter(ScribeHandler.class, "Shard Slot Update Meter", "Updated", TimeUnit.SECONDS);
    private final Timer synchronizationTimer = Metrics.newTimer(RollupService.class, "ScheduleSlotsOlderThan Synchronization Wait Timer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS); // TODO: remove this metric once we determine whether or not synchronization is causing issues

    //
    // state
    //
    
    // this is where the last modification times for shards is kept.  The map is in this manner:
    // shard -> granularity -> slot -> timestamp
    // I am not proud of it.
    private final Map<Integer, Map<Granularity, Map<Integer, UpdateStamp>>> shardUpdates = new HashMap<Integer, Map<Granularity, Map<Integer, UpdateStamp>>>(); // don't hate.
    
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
        this.currentTimeMillis = currentTimeMillis;
        this.managedShards = new HashSet<Integer>(managedShards);
        this.lockManager = new NoOpShardLockManager();

        initUpdateMapForShards(ALL_SHARDS);
    }

    ScheduleContext(long currentTimeMillis, Collection<Integer> managedShards, String zookeeperCluster) {
        this(currentTimeMillis, managedShards);

        this.lockManager = new ZKBasedShardLockManager(zookeeperCluster, this.managedShards);
    }

    private void initUpdateMapForShards(Set<Integer> shards) {
        // create an update map for every shard.
        for (int i : shards) {
            Map<Granularity, Map<Integer, UpdateStamp>> updates = new HashMap<Granularity, Map<Integer, UpdateStamp>>();
            for (Granularity g : GRANULARITIES)
                updates.put(g, Collections.synchronizedMap(new HashMap<Integer, UpdateStamp>()));
            this.shardUpdates.put(i, updates);
        }
    }

    void setCurrentTimeMillis(long millis){ currentTimeMillis = millis; }
    public long getCurrentTimeMillis() { return currentTimeMillis; }
    
    // marks slots dirty. -- This is ONLY called on a subset of host environments where Blueflood runs
    // -- it runs on scribe nodes, not dcass nodes
    public void update(long millis, int shard) {
        // there are two update paths. for managed shards, we must guard the scheduled and running collections.  but
        // for unmanaged shards, we just let the update happen uncontested.
        if (log.isTraceEnabled())
            log.trace("Updating {} to {}", shard, millis);
        boolean isManaged = managedShards.size() > 0 && managedShards.contains(shard);
        for (Granularity g : GRANULARITIES) {
            Map<Integer, UpdateStamp> updateStampsBySlotMap = shardUpdates.get(shard).get(g);
            int slot = g.slot(millis);
            if (isManaged) {
                synchronized (scheduledSlots) { // write.
                    if (scheduledSlots.remove(g.formatLocatorKey(slot, shard)) && log.isDebugEnabled()) {
                        log.debug("descheduled " + g.formatLocatorKey(slot, shard));// don't worry about orderedScheduledSlots
                    }
                    createOrUpdateUpdateStampMapForSlotAndMillisecond(updateStampsBySlotMap, slot, millis);
                }
            } else {
                createOrUpdateUpdateStampMapForSlotAndMillisecond(updateStampsBySlotMap, slot, millis);
            }
        }
    }

    private void createOrUpdateUpdateStampMapForSlotAndMillisecond(Map<Integer, UpdateStamp> updateStampMap, int slot, long millis) {
        synchronized (updateStampMap) {
            if (updateStampMap.containsKey(slot)) {
                UpdateStamp stamp = updateStampMap.get(slot);
                stamp.setTimestamp(millis);
                stamp.setState(UpdateStamp.State.Active);
                stamp.setDirty(true);
            } else {
                updateStampMap.put(slot, new UpdateStamp(millis, UpdateStamp.State.Active, true));
            }
        }
        updateStampMeter.mark();
    }

    // iterates over all active slots, scheduling those that haven't been updated in maxAgeMillis.
    // only one thread should be calling in this puppy.
    void scheduleSlotsOlderThan(long maxAgeMillis) {
        long now = currentTimeMillis;
        ArrayList<Integer> shardKeys = new ArrayList<Integer>(managedShards);
        Collections.shuffle(shardKeys);

        shard_iteration : for (int shard : shardKeys) {
            for (Granularity g : GRANULARITIES) {
                // sync on map since we do not want anything added to or taken from it while we iterate.
                TimerContext ctx = synchronizationTimer.time();
                synchronized (scheduledSlots) { // read
                    synchronized (runningSlots) { // read
                        Map<Integer, UpdateStamp> updateStampsBySlotMap = shardUpdates.get(shard).get(g);
                        synchronized (updateStampsBySlotMap) { // read
                            ctx.stop();
                            active_keys: for (Map.Entry<Integer, UpdateStamp> slotToUpdateStamp : updateStampsBySlotMap.entrySet()) {
                                final long timeElapsed = now - slotToUpdateStamp.getValue().getTimestamp();
                                timeSinceUpdate.update(timeElapsed);
                                if (slotToUpdateStamp.getValue().getState() == UpdateStamp.State.Rolled) {
                                    continue active_keys;
                                }
                                if (timeElapsed <= maxAgeMillis) {
                                    continue active_keys;
                                }
                                if (!canWorkOnShard(shard)) {
                                    continue shard_iteration;
                                }
                                if (areChildKeysOrSelfKeyScheduledOrRunning(shard, g, slotToUpdateStamp.getKey())) {
                                    continue active_keys;
                                }
                                String key = g.formatLocatorKey(slotToUpdateStamp.getKey(), shard);
                                // write
                                scheduledSlots.add(key);
                                orderedScheduledSlots.add(key);
                                recentlyScheduledShards.put(shard, currentTimeMillis);
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean areChildKeysOrSelfKeyScheduledOrRunning(int shard, Granularity g, int slot) {
        String key = g.formatLocatorKey(slot, shard);

        // if any ineligible (children and self) keys are running or scheduled to run, we shouldn't work on this.
        Collection<String> ineligibleKeys = new ArrayList<String>();
        ineligibleKeys.addAll(g.getChildrenKeys(slot, shard));
        ineligibleKeys.add(key);

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
                // notice how we changed the state, but the timestamp remained the same. this is important.  When the
                // state is evaluated (i.e., in Reader.getShardState()) we need to realize that when timstamps are the
                // same (this will happen), that a remove always wins during the coalesce.
                scheduledSlots.remove(key);
                UpdateStamp stamp = shardUpdates.get(shard).get(gran).get(slot); // sync aware!
                // no need to set dirty/clean here.
                stamp.setState(UpdateStamp.State.Running);
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
                shardUpdates.get(shard).get(gran).get(slot).setState(UpdateStamp.State.Active);
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
            UpdateStamp stamp = shardUpdates.get(shard).get(gran).get(slot); 
            stamp.setState(UpdateStamp.State.Rolled);
            stamp.setTimestamp(System.currentTimeMillis());
            stamp.setDirty(true);
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
    
    // return the shards this node is responsible for.
    // will be part of jmx.
    Collection<Integer> getManagedShards() {
        return Collections.unmodifiableCollection(this.managedShards);
    }
    
    // gets a snapshot of the last updates for a particular (granularity, shard).
    public Map<Integer, UpdateStamp> getSlotStamps(Granularity gran, int shard) {
        // essentially a copy on read map.
        final Map<Granularity, Map<Integer, UpdateStamp>> map = shardUpdates.get(shard);

        if (map != null) {
            final Map<Integer, UpdateStamp> slotStampMap = map.get(gran);
            if (slotStampMap != null) {
                synchronized (slotStampMap) {
                    return Collections.unmodifiableMap(new HashMap<Integer, UpdateStamp>(slotStampMap));
                }
            }
        }

        return null;
    }
    
    // gets all the dirty updates for a particular (granularity, shard).
    public Map<Integer, UpdateStamp> getDirtySlotStamps(Granularity gran, int shard) {
        Map<Integer, UpdateStamp> map = shardUpdates.get(shard).get(gran);
        HashMap<Integer, UpdateStamp> ret = new HashMap<Integer, UpdateStamp>();
        synchronized (map) {
            for (Map.Entry<Integer, UpdateStamp> entry : map.entrySet())
                if (entry.getValue().isDirty())
                    ret.put(entry.getKey(), entry.getValue());
        }
        return ret;
    }
    
    // write updated timestamps to the memory structure.
    // since these updates come from a DB, don't mark them as dirty.
    void applySlotStamps(Granularity gran, int shard, Map<Integer, UpdateStamp> updates) {
        if (updates.size() == 0) return;
        final TimerContext ctx = applySlotStampsTimer.time();
        Map<Integer, UpdateStamp> map = shardUpdates.get(shard).get(gran);
        synchronized (map) {
            for (Map.Entry<Integer, UpdateStamp> entry : updates.entrySet()) {
                // We iterate through updates. if we see one that has a later timestamp than what we already have, we 
                // replace what we have. We never remove.
                UpdateStamp cur = map.get(entry.getKey());
                if (cur == null) {
                    // easy case: we're not already tracking it. add it to the map even if it is a remove.
                    map.put(entry.getKey(), entry.getValue());
                    // else if is is a remove, there is nothing to remove from map, so do nothing.
                } else {
                    // figure out if we need to update.  This happens in two cases:
                    if (cur.getTimestamp() < entry.getValue().getTimestamp()) {
                        // 1) if current value is older than the value being applied.
                        cur.setTimestamp(entry.getValue().getTimestamp());
                        cur.setState(entry.getValue().getState());
                    } else if (cur.getTimestamp() == entry.getValue().getTimestamp() && !entry.getValue().equals(cur) && entry.getValue().getState() == UpdateStamp.State.Rolled) {
                        // 2) if current value is same but value being applied is a remove.
                        cur.setTimestamp(entry.getValue().getTimestamp());
                        cur.setState(entry.getValue().getState());
                    }
                }
            }
        }
        ctx.stop();
    }

    // precondition: shard is unmanaged.
    void addShard(int shard) {
        if (managedShards.contains(shard))
            return;
        managedShards.add(shard);
        lockManager.addShard(shard);    
    }
    
    // precondition: shard is managed.
    void removeShard(int shard) {
        if (!managedShards.contains(shard))
            return;
        managedShards.remove(shard);
        lockManager.removeShard(shard);
    }

    Histogram getRollupWaitHist() { return rollupWaitHist; }
    Timer getRollupExecuteTimer() { return rollupExecuteTimer; }
    Timer getRollupLocatorExecuteTimer() { return rollupLocatorExecuteTimer; }
    
    Set<Integer> getRecentlyScheduledShards() {
        // Collections.unmodifiableSet(...) upsets JMX.
        return new TreeSet<Integer>(recentlyScheduledShards.asMap().keySet());
    }
    
    public Ticker asTicker() {
        return new Ticker() {
            @Override
            public long read() {
                return ScheduleContext.this.getCurrentTimeMillis();
            }
        };
    }
}
