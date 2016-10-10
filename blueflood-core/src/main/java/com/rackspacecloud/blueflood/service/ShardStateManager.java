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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.google.common.base.Ticker;
import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ShardStateManager {
    private static final Logger log = LoggerFactory.getLogger(ShardStateManager.class);
    private static final Set<Integer> ALL_SHARDS = new HashSet<Integer>(Util.parseShards("ALL"));
    final Set<Integer> shards; // Managed shards
    final Map<Integer, ShardToGranularityMap> shardToGranularityStates = new HashMap<Integer, ShardToGranularityMap>();
    private final Ticker serverTimeMillisecondTicker;
    private static final long millisInADay = 24 * 60 * 60 * 1000;

    private static final Histogram timeSinceUpdate = Metrics.histogram(RollupService.class, "Shard Slot Time Elapsed scheduleEligibleSlots");
    // todo: CM_SPECIFIC verify changing metric class name doesn't break things.
    private static final Meter updateStampMeter = Metrics.meter(ShardStateManager.class, "Shard Slot Update Meter");
    private final Meter parentBeforeChild = Metrics.meter(RollupService.class, "Parent slot executed before child");
    private static final Map<Granularity, Meter> granToReRollMeters = new HashMap<Granularity, Meter>();
    private static final Map<Granularity, Meter> reRollForShortDelayMetricsMeters = new HashMap<Granularity, Meter>();
    private static final Map<Granularity, Meter> reRollForLongDelayMetricsMeters = new HashMap<Granularity, Meter>();
    private static final Map<Granularity, Meter> granToDelayedMetricsMeter = new HashMap<Granularity, Meter>();

    // If there are no delayed metrics, a slot should only get rolled up again after 14 days since its last rollup. Since we only allow
    // delayed data that is BEFORE_CURRENT_COLLECTIONTIME_MS (typically 3 days) old, we are assuming that if rollup happens again within
    // that time, its a re-roll because of delayed data.
    public static final long REROLL_TIME_SPAN_ASSUMED_VALUE = Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );

    private final Clock clock;

    static {
        for (Granularity rollupGranularity : Granularity.rollupGranularities()) {
            granToReRollMeters.put(rollupGranularity, Metrics.meter(RollupService.class, String.format("%s Re-rolling up because of delayed metrics", rollupGranularity.shortName())));
            granToDelayedMetricsMeter.put(rollupGranularity, Metrics.meter(RollupService.class, String.format("Delayed metric that has a danger of TTLing", rollupGranularity.shortName())));

            reRollForShortDelayMetricsMeters.put(rollupGranularity, Metrics.meter(RollupService.class, String.format("%s Slots waiting to be re-rolled because of short delay metrics", rollupGranularity.shortName())));
            reRollForLongDelayMetricsMeters.put(rollupGranularity, Metrics.meter(RollupService.class, String.format("%s Slots waiting to be re-rolled because of long delay metrics", rollupGranularity.shortName())));
        }
    }

    protected ShardStateManager(Collection<Integer> shards, Ticker ticker) {
        this(shards, ticker, new DefaultClockImpl());
    }

    protected ShardStateManager(Collection<Integer> shards, Ticker ticker, Clock clock) {
        this.shards = new HashSet<Integer>(shards);
        for (Integer shard : ALL_SHARDS) { // Why not just do this for managed shards?
            shardToGranularityStates.put(shard, new ShardToGranularityMap(shard));
        }
        this.serverTimeMillisecondTicker = ticker;
        this.clock = clock;
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

    protected UpdateStamp getUpdateStamp(SlotKey slotKey) {
        SlotStateManager slotStateManager = this.getSlotStateManager(slotKey.getShard(), slotKey.getGranularity());
        UpdateStamp stamp = slotStateManager.slotToUpdateStampMap.get(slotKey.getSlot());
        return stamp;

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

    public void updateSlotOnRead(int shard, SlotState slotState) {
        getSlotStateManager(shard, slotState.getGranularity()).updateSlotOnRead(slotState);
    }

    public void setAllCoarserSlotsDirtyForSlot(SlotKey slotKey) {
        boolean done = false;
        Granularity coarserGran = slotKey.getGranularity();
        int coarserSlot = slotKey.getSlot();

        while (!done) {
            try {
                coarserGran = coarserGran.coarser();
                coarserSlot = coarserGran.slotFromFinerSlot(coarserSlot);
                ConcurrentMap<Integer, UpdateStamp> updateStampsBySlotMap = getSlotStateManager(slotKey.getShard(), coarserGran).slotToUpdateStampMap;
                UpdateStamp coarseSlotStamp = updateStampsBySlotMap.get(coarserSlot);

                if (coarseSlotStamp == null) {
                    log.debug("No stamp for coarser slot: {}; supplied slot: {}",
                            SlotKey.of(coarserGran, coarserSlot, slotKey.getShard()),
                            slotKey);
                    updateStampsBySlotMap.putIfAbsent(coarserSlot,
                            new UpdateStamp(serverTimeMillisecondTicker.read(), UpdateStamp.State.Active, true));
                    continue;
                }

                UpdateStamp.State coarseSlotState = coarseSlotStamp.getState();
                if (coarseSlotState != UpdateStamp.State.Active) {
                    parentBeforeChild.mark();
                    log.debug("Coarser slot not in active state when finer slot {} just got rolled up. Marking coarser slot {} dirty.",
                            slotKey, SlotKey.of(coarserGran, coarserSlot, slotKey.getShard()));
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
        final ConcurrentMap<Integer, UpdateStamp> slotToUpdateStampMap;

        protected SlotStateManager(int shard, Granularity granularity) {
            this.shard = shard;
            this.granularity = granularity;
            slotToUpdateStampMap = new ConcurrentHashMap<Integer, UpdateStamp>(granularity.numSlots());
        }

        /**
         *
         * Imagine metrics are flowing in from multiple ingestor nodes. The
         * ingestion path updates schedule context while writing metrics to
         * cassandra.(See
         * {@link com.rackspacecloud.blueflood.inputs.processors.BatchWriter BatchWriter}).
         * We cannot make any ordering guarantees on the metrics. So every
         * metric that comes in updates the slot state to its collection time.
         * <p>
         *
         * This state gets pushed in cassandra by {@link ShardStatePusher} and read on
         * the rollup slave. Rollup slave is going to update its state to
         * {@link com.rackspacecloud.blueflood.service.UpdateStamp.State#Active ACTIVE}
         * as long as the timestamp does not match. Rollup slave shard
         * map can be in 3 states:
         * 1) {@link com.rackspacecloud.blueflood.service.UpdateStamp.State#Active Active}
         * 2) {@link com.rackspacecloud.blueflood.service.UpdateStamp.State#Rolled Rolled}
         * 3) {@link com.rackspacecloud.blueflood.service.UpdateStamp.State#Running Running}.
         * Every {@code ACTIVE} update is taken for Rolled and Running states,
         * but if the shard map is already in an {@code ACTIVE} state, then the
         * update happens only if the timestamp of update coming in is greater
         * than what we have. On Rollup slave it means eventually when it rolls
         * up data for the {@code ACTIVE} slot, it will be marked with the
         * collection time belonging to a metric which was generated later.
         * <p>
         *
         * For a case of multiple ingestors, it means eventually higher
         * timestamp will win, and will be updated even if that ingestor did
         * not receive metric with that timestamp and will stop triggering the
         * state to {@code ACTIVE} on rollup host. After this convergence is
         * reached the last rollup time match with the last active times on all
         * ingestor nodes.
         *
         * LastUpdateTime of an active slot is used as last ingest time and lastUpdateTime
         * of a rolled slot is used as last rollup time. UpdateStamp which is in memory
         * for each slot has the last ingest time and last rollup time of that slot. 
         */
        protected void updateSlotOnRead(SlotState slotState) {
            final int slot = slotState.getSlot();
            final long timestamp = slotState.getTimestamp();
            UpdateStamp.State state = slotState.getState();

            //For slots in state "A", this would be last ingest time
            //For slots in state "X", this would be last rollup time
            final long lastUpdateTimestamp = slotState.getLastUpdatedTimestamp();

            UpdateStamp stampInMemory = slotToUpdateStampMap.get(slot);
            if (stampInMemory == null) {
                // haven't seen this slot before, take the update. This happens when a blueflood service is just started.
                slotToUpdateStampMap.put(slot, new UpdateStamp(timestamp, state, false, 0, lastUpdateTimestamp));
            } else if (stampInMemory.getTimestamp() != timestamp && state.equals(UpdateStamp.State.Active)) {
                // 1) new update coming in. We can be in 3 states 1) Active 2) Rolled 3) Running. Apply the update in all cases except when we are already active and
                //    the triggering timestamp we have is greater or the stampInMemory is yet to be persisted i.e still dirty

                // This "if" is equivalent to: 
                //  if (current is not active) || (current is older && clean)
                if (!(stampInMemory.getState().equals(UpdateStamp.State.Active) && (stampInMemory.getTimestamp() > timestamp || stampInMemory.isDirty()))) {
                    slotToUpdateStampMap.put(slot, new UpdateStamp(timestamp, state, false, stampInMemory.getLastRollupTimestamp(), lastUpdateTimestamp));
                } else {
                    // keep rewriting the newer timestamp, in case it has been overwritten:
                    stampInMemory.setDirty(true); // This is crucial for convergence, we need to superimpose a higher timestamp which can be done only if we set it to dirty
                }
            } else if (stampInMemory.getTimestamp() == timestamp && state.equals(UpdateStamp.State.Rolled)) {
                // 2) if current value is same but value being applied is a remove, remove wins.
                stampInMemory.setState(UpdateStamp.State.Rolled);

                //For incoming update(from metrics_state) of "Rolled" status, we use its last updated time as the last rollup time.
                if (lastUpdateTimestamp > stampInMemory.getLastRollupTimestamp())
                    stampInMemory.setLastRollupTimestamp(lastUpdateTimestamp);
            } else if (state.equals(UpdateStamp.State.Rolled)) {

                //For incoming update(from metrics_state) of "Rolled" status, we use its last updated time as the last rollup time.
                if (lastUpdateTimestamp > stampInMemory.getLastRollupTimestamp())
                    stampInMemory.setLastRollupTimestamp(lastUpdateTimestamp);
            }
        }

        protected void createOrUpdateForSlotAndMillisecond(int slot, long millis) {
            long nowMillis = clock.now().getMillis();
            if (slotToUpdateStampMap.containsKey(slot)) {
                UpdateStamp stamp = slotToUpdateStampMap.get(slot);
                stamp.setTimestamp(millis);

                // Temporarily setting last ingest time to current time until we get more accurate value from db.
                // This will not be persisted.
                stamp.setLastIngestTimestamp(nowMillis);

                stamp.setState(UpdateStamp.State.Active);
                stamp.setDirty(true);
            } else {
                slotToUpdateStampMap.put(slot, new UpdateStamp(millis, UpdateStamp.State.Active, true, 0, nowMillis));
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

        /**
         * Determines if a slot is being re-rolled or not.
         *
         * Since we only allow delayed metrics upto 3 days(BEFORE_CURRENT_COLLECTIONTIME_MS), a slot can be
         * identified as being re-rolled, if the last rollup is within those last 3 days.
         *
         * @param slot
         * @param now
         * @return
         */
        protected boolean isReroll(int slot, long now) {
            final UpdateStamp updateStamp = slotToUpdateStampMap.get(slot);
            final long timeElapsedSinceLastRollup = now - updateStamp.getLastRollupTimestamp();

            if (updateStamp.getLastRollupTimestamp() > 0 &&
                    timeElapsedSinceLastRollup < REROLL_TIME_SPAN_ASSUMED_VALUE) {
                return true;
            }

            return false;
        }

        /**
         * A slot will become eligible for rollup/re-roll based on the below three configs.
         *
         * 1) 1st rollup  -> Eligible after ROLLUP_DELAY_MILLIS from collection time.
         * 2) 1st re-roll -> This happens for metrics with short delay (within SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS).
         *                   Eligible after SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS from collection time.
         * 3) nth re-roll -> This happens for metrics with long delay(more than SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS).
         *                   Eligible after LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS from last ingest time.
         *                   This re-roll repeats as we keep getting delayed metrics.
         *
         *
         *     |<-- SHORT_DELAY_METRICS_ROLLUP_DELAY--->|
         *     |                                        |
         *     |<-- ROLLUP_DELAY--->|                   |     |<--ROLLUP_WAIT-->|                |<--ROLLUP_WAIT-->|
         *     |                    |                   |     |                 |                |                 |
         * -------------------------------------------------------------------------------------------------------------
         * |slot|                   ^          ^        ^     ^                 ^    ^   ^   ^   ^                 ^
         * | X  |                   |          |        |     |                 |    |   |   |   |                 |
         *                          |   delayed metric  | delayed metric        |   delayed metrics                |
         *                          |                   |                       |                                  |
         *                      1st rollup         1st re-roll              2nd re-roll                      3rd re-roll
         *
         *
         * @param now is current time
         * @param maxAgeMillis is ROLLUP_DELAY_MILLIS
         * @param rollupDelayForMetricsWithShortDelay is SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS
         * @param rollupWaitForMetricsWithLongDelay is LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS
         * @return list of slots that are eligible for rollup
         */
        protected List<Integer> getSlotsEligibleForRollup(long now,
                                                          long maxAgeMillis,
                                                          long rollupDelayForMetricsWithShortDelay,
                                                          long rollupWaitForMetricsWithLongDelay) {
            List<Integer> outputKeys = new ArrayList<Integer>();
            long nowMillis = clock.now().getMillis();
            for (Map.Entry<Integer, UpdateStamp> entry : slotToUpdateStampMap.entrySet()) {
                final int slot = entry.getKey();
                final UpdateStamp update = entry.getValue();
                final long timeElapsed = now - update.getTimestamp();
                timeSinceUpdate.update(timeElapsed);

                if (update.getState() == UpdateStamp.State.Rolled) {
                    continue;
                }
                if (timeElapsed <= maxAgeMillis) {
                    continue;
                }

                //Handling re-rolls:
                if (isReroll(slot, now)) {
                    SlotKey slotKey = SlotKey.of(granularity, entry.getKey(), shard);

                    //short delay
                    if (timeElapsed <= rollupDelayForMetricsWithShortDelay) {

                        reRollForShortDelayMetricsMeters.get(granularity).mark();
                        log.debug(String.format("Short delay: Delaying re-roll of slotKey [%s] as [%d] millis " +
                                "haven't elapsed since collection time:[%d] now: [%d] time elapsed: [%d] last " +
                                "rollup time: [%d]", slotKey, rollupDelayForMetricsWithShortDelay,
                                update.getTimestamp(), now, timeElapsed, update.getLastRollupTimestamp()));
                        continue;
                    }

                    if (update.getLastIngestTimestamp() > 0 ) {
                        long delayOfLastIngestedMetric = update.getLastIngestTimestamp() - update.getTimestamp();
                        final long timeElapsedSinceLastIngest = now - update.getLastIngestTimestamp();

                        //long delay
                        if (delayOfLastIngestedMetric > rollupDelayForMetricsWithShortDelay &&
                                timeElapsedSinceLastIngest <= rollupWaitForMetricsWithLongDelay) {

                            reRollForLongDelayMetricsMeters.get(granularity).mark();
                            log.debug(String.format("Long delay: Delaying re-roll of slotKey [%s] as we received " +
                                            "delayed metrics within the last [%d] millis with rollup_wait of [%d] millis. last " +
                                            "ingest time: [%d]", slotKey, timeElapsedSinceLastIngest,
                                    rollupWaitForMetricsWithLongDelay, update.getLastIngestTimestamp()));
                            continue;
                        }
                    }

                    granToReRollMeters.get(granularity).mark();
                    if (nowMillis - update.getTimestamp() >= millisInADay) {
                        granToDelayedMetricsMeter.get(granularity).mark();
                    }
                }
                outputKeys.add(entry.getKey());
            }

            return outputKeys;
        }
    }
}


