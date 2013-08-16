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

import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.utils.Util;
import com.google.common.base.Ticker;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ShardStateManager {
    private static final Logger log = LoggerFactory.getLogger(ShardStateManager.class);
    private static final Set<Integer> ALL_SHARDS = new HashSet<Integer>(Util.parseShards("ALL"));
    final Set<Integer> shards; // Managed shards
    final Map<Integer, ShardToGranularityMap> shardToGranularityStates = new HashMap<Integer, ShardToGranularityMap>();
    private final Ticker serverTimeMillisecondTicker;

    private static final Histogram timeSinceUpdate = Metrics.newHistogram(RollupService.class, "Shard Slot Time Elapsed scheduleSlotsOlderThan", true);
    // todo: CM_SPECIFIC verify changing metric class name doesn't break things.
    private static final Meter updateStampMeter = Metrics.newMeter(ShardStateManager.class, "Shard Slot Update Meter", "Updated", TimeUnit.SECONDS);
    private final Meter parentBeforeChild = Metrics.newMeter(RollupService.class, "Parent slot executed before child", "OutOfOrderSchedule", TimeUnit.SECONDS);
    private static final Meter reRollupData = Metrics.newMeter(RollupService.class, "Re-rolling up a slot because of new data", "Slot", TimeUnit.SECONDS);

    protected ShardStateManager(Collection<Integer> shards, Ticker ticker) {
        this.shards = new HashSet<Integer>(shards);
        for (Integer shard : ALL_SHARDS) { // Why not just do this for managed shards?
            shardToGranularityStates.put(shard, new ShardToGranularityMap(shard));
        }
        this.serverTimeMillisecondTicker = ticker;
    }

    protected Collection<Integer> getManagedShards() {
        return Collections.unmodifiableCollection(this.shards);
    }

    protected Boolean contains(int shard) {
        return shards.size() != 0 && shards.contains(shard);
    }

    protected void add(int shard) {
        if (contains(shard))
            return;
        shards.add(shard);
    }

    protected void remove(int shard) {
        if (!contains(shard))
            return;
        this.shards.remove(shard);
    }

    public SlotStateManager getSlotStateManager(int shard, Granularity granularity) {
        return shardToGranularityStates.get(shard).granularityToSlots.get(granularity);
    }

    protected UpdateStamp getUpdateStamp(int shard, Granularity gran, int slot) {
        return this.getSlotStateManager(shard, gran).slotToUpdateStampMap.get(slot);
    }

    // Side effect: mark dirty slots as clean
    protected Map<Granularity, Map<Integer, UpdateStamp>> getDirtySlotsToPersist(int shard) {
        Map<Granularity, Map<Integer, UpdateStamp>> slotTimes = new HashMap<Granularity, Map<Integer, UpdateStamp>>();
        int numUpdates = 0;
        for (Granularity gran : Granularity.rollupGranularities()) {
            Map<Integer, UpdateStamp> dirty = getSlotStateManager(shard, gran).getDirtySlotStampsAndMarkClean();
            slotTimes.put(gran, dirty);

            if (dirty.size() > 0) {
                numUpdates += dirty.size();
            }
        }
        if (numUpdates > 0) {
            // for updates that come by way of scribe, you'll typically see 5 as the number of updates (one for
            // each granularity).  On rollup slaves the situation is a bit different. You'll see only the slot
            // of the granularity just written to marked dirty (so 1).
            log.debug("Found {} dirty slots for shard {}", new Object[]{numUpdates, shard});
            return slotTimes;
        }
        return null;
    }

    public void updateSlotOnRead(int shard, Granularity g, int slot, long timestamp, UpdateStamp.State state) {
        getSlotStateManager(shard, g).updateSlotOnRead(slot, timestamp, state);
    }

    public void setAllCoarserSlotsDirtyForChildSlot(int shard, Granularity childGran,
                                                    int childslot, long childTimestamp) {
        boolean done = false;
        Granularity coarserGran = childGran;

        while (!done) {
            try {
                coarserGran = coarserGran.coarser();
                int coarseSlot = coarserGran.slot(childTimestamp);
                Map<Integer, UpdateStamp> updateStampsBySlotMap = getSlotStateManager(shard, coarserGran).slotToUpdateStampMap;
                UpdateStamp coarseSlotStamp = updateStampsBySlotMap.get(coarseSlot);
                UpdateStamp.State coarseSlotState = coarseSlotStamp.getState();

                if (coarseSlotState != UpdateStamp.State.Active) {
                    parentBeforeChild.mark();
                    log.debug("Parent slot not in active state when child slot "
                            + childGran.formatLocatorKey(childslot, shard)
                            + " just got rolled up. Marking parent slot "
                            + coarserGran.formatLocatorKey(coarseSlot, shard) + " dirty");
                    coarseSlotStamp.setState(UpdateStamp.State.Active);
                    coarseSlotStamp.setDirty(true);
                    coarseSlotStamp.setTimestamp(serverTimeMillisecondTicker.read());
                }
            } catch (GranularityException ex) {
                done = true;
            }
        }
    }

    private class ShardToGranularityMap {
        final int shard;
        final Map<Granularity, SlotStateManager> granularityToSlots = new HashMap<Granularity, SlotStateManager>();

        protected ShardToGranularityMap(int shard) {
            this.shard = shard;

            for (Granularity granularity : Granularity.rollupGranularities()) {
                granularityToSlots.put(granularity, new SlotStateManager(shard, granularity));
            }
        }
    }

    protected class SlotStateManager {
        private final int shard;
        final Granularity granularity;
        final Map<Integer, UpdateStamp> slotToUpdateStampMap;

        protected SlotStateManager(int shard, Granularity granularity) {
            this.shard = shard;
            this.granularity = granularity;
            slotToUpdateStampMap = new ConcurrentHashMap<Integer, UpdateStamp>(granularity.numSlots());
        }

        protected void updateSlotOnRead(int slot, long timestamp, UpdateStamp.State state) {
            UpdateStamp stamp = slotToUpdateStampMap.get(slot);
            if (stamp == null) {
                // haven't seen this slot before, take the update
                slotToUpdateStampMap.put(slot, new UpdateStamp(timestamp, state, false));
            } else if (stamp.getTimestamp() < timestamp) {
                // 1) if current value is older than the value being applied.
                slotToUpdateStampMap.put(slot, new UpdateStamp(timestamp, state, false));
            } else if (stamp.getTimestamp() == timestamp && state.equals(UpdateStamp.State.Rolled)) {
                // 2) if current value is same but value being applied is a remove, remove wins.
                stamp.setState(UpdateStamp.State.Rolled);
            }
        }

        protected void createOrUpdateForSlotAndMillisecond(int slot, long millis) {
            if (slotToUpdateStampMap.containsKey(slot)) {
                UpdateStamp stamp = slotToUpdateStampMap.get(slot);
                stamp.setTimestamp(millis);
                if (stamp.getState().equals(UpdateStamp.State.Rolled)) {
                    reRollupData.mark();
                }
                stamp.setState(UpdateStamp.State.Active);
                stamp.setDirty(true);
            } else {
                slotToUpdateStampMap.put(slot, new UpdateStamp(millis, UpdateStamp.State.Active, true));
            }
            updateStampMeter.mark();
        }

        protected Map<Integer, UpdateStamp> getDirtySlotStampsAndMarkClean() {
            HashMap<Integer, UpdateStamp> dirtySlots = new HashMap<Integer, UpdateStamp>();
            for (Map.Entry<Integer, UpdateStamp> entry : slotToUpdateStampMap.entrySet()) {
                if (entry.getValue().isDirty()) {
                    dirtySlots.put(entry.getKey(), entry.getValue());
                    entry.getValue().setDirty(false);
                }
            }
            return dirtySlots;
        }

        protected UpdateStamp getAndSetState(int slot, UpdateStamp.State state) {
            UpdateStamp stamp = slotToUpdateStampMap.get(slot);
            stamp.setState(state);
            return stamp;
        }

        // gets a snapshot of the last updates
        protected Map<Integer, UpdateStamp> getSlotStamps() {
            // essentially a copy on read map.
            return Collections.unmodifiableMap(slotToUpdateStampMap);
        }

        protected List<Integer> getSlotsOlderThan(long now, long maxAgeMillis) {
            List<Integer> outputKeys = new ArrayList<Integer>();
            for (Map.Entry<Integer, UpdateStamp> entry : slotToUpdateStampMap.entrySet()) {
                final UpdateStamp update = entry.getValue();
                final long timeElapsed = now - update.getTimestamp();
                timeSinceUpdate.update(timeElapsed);
                if (update.getState() == UpdateStamp.State.Rolled) {
                    continue;
                }
                if (timeElapsed <= maxAgeMillis) {
                    continue;
                }
                outputKeys.add(entry.getKey());
            }
            return outputKeys;
        }

        protected Collection<String> getChildAndSelfKeysForSlot(int slot) {
            Collection<String> keys = new ArrayList<String>();
            keys.addAll(this.granularity.getChildrenKeys(slot, shard));
            keys.add(granularity.formatLocatorKey(slot, shard));
            return keys;
        }
    }
}


