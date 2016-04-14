package com.rackspacecloud.blueflood.service;


import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SlotStateManager {
    private static final Logger log = LoggerFactory.getLogger(SlotStateManager.class);

    static final Histogram timeSinceUpdate = Metrics.histogram(RollupService.class, "Shard Slot Time Elapsed scheduleEligibleSlots");
    // todo: CM_SPECIFIC verify changing metric class name doesn't break things.
    static final Meter updateStampMeter = Metrics.meter(ShardStateManager.class, "Shard Slot Update Meter");
    static final Map<Granularity, Meter> granToReRollMeters = new HashMap<Granularity, Meter>();
    static final Map<Granularity, Meter> granToDelayedMetricsMeter = new HashMap<Granularity, Meter>();

    static final long DELAYED_METRICS_MAX_ALLOWED_DELAY = Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );

    static {
        for (Granularity rollupGranularity : Granularity.rollupGranularities()) {
            granToReRollMeters.put(rollupGranularity, Metrics.meter(RollupService.class, String.format("%s Re-rolling up because of delayed metrics", rollupGranularity.shortName())));
            granToDelayedMetricsMeter.put(rollupGranularity, Metrics.meter(RollupService.class, String.format("Delayed metric that has a danger of TTLing", rollupGranularity.shortName())));
        }
    }

    private final int shard;
    final Granularity granularity;
    final ConcurrentMap<Integer, UpdateStamp> slotToUpdateStampMap;
    private static final long millisInADay = 24 * 60 * 60 * 1000;
    private final ShardStateManager parent;

    protected SlotStateManager(ShardStateManager parent, int shard, Granularity granularity) {
        this.parent = parent;
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
     */
    protected void updateSlotOnRead(SlotState slotState) {
        final int slot = slotState.getSlot();
        final long timestamp = slotState.getTimestamp();
        UpdateStamp.State state = slotState.getState();
        final long lastUpdateTimestamp = slotState.getLastUpdatedTimestamp();

        UpdateStamp stamp = slotToUpdateStampMap.get(slot);
        long nowMillis = new DateTime().getMillis();
        if (stamp == null) {
            // haven't seen this slot before, take the update. This happens when a blueflood service is just started.
            slotToUpdateStampMap.put(slot, new UpdateStamp(timestamp, state, false));
        } else if (stamp.getTimestamp() != timestamp && state.equals(UpdateStamp.State.Active)) {
            // 1) new update coming in. We can be in 3 states 1) Active 2) Rolled 3) Running. Apply the update in all cases except when we are already active and
            //    the triggering timestamp we have is greater or the stamp in memory is yet to be persisted i.e still dirty

            // This "if" is equivalent to:
            //  if (current is not active) || (current is older && clean)
            if (!(stamp.getState().equals(UpdateStamp.State.Active) && (stamp.getTimestamp() > timestamp || stamp.isDirty()))) {
                // If the shard state we have is ROLLED, and the snapped millis for the last rollup time and the current update is same, then its a re-roll
                if (stamp.getState().equals(UpdateStamp.State.Rolled) && granularity.snapMillis(stamp.getTimestamp()) == granularity.snapMillis(timestamp)) {
                    granToReRollMeters.get(granularity).mark();
                    if (nowMillis - timestamp >= millisInADay) {
                        granToDelayedMetricsMeter.get(granularity).mark();
                    }
                }

                slotToUpdateStampMap.put(slot, new UpdateStamp(timestamp, state, false, stamp.getLastRollupTimestamp()));
            } else {
                // keep rewriting the newer timestamp, in case it has been overwritten:
                stamp.setDirty(true); // This is crucial for convergence, we need to superimpose a higher timestamp which can be done only if we set it to dirty
            }
        } else if (stamp.getTimestamp() == timestamp && state.equals(UpdateStamp.State.Rolled)) {
            // 2) if current value is same but value being applied is a remove, remove wins.
            stamp.setState(UpdateStamp.State.Rolled);
            //For incoming update(from metrics_state) of "Rolled" status, we use its last updated time as the last rollup time.
            stamp.setLastRollupTimestamp(lastUpdateTimestamp);
        } else if (state.equals(UpdateStamp.State.Rolled)) {

            //For incoming update(from metrics_state) of "Rolled" status, we use its last updated time as the last rollup time.
            stamp.setLastRollupTimestamp(lastUpdateTimestamp);
        }
    }

    protected void createOrUpdateForSlotAndMillisecond(int slot, long millis) {
        if (slotToUpdateStampMap.containsKey(slot)) {
            UpdateStamp stamp = slotToUpdateStampMap.get(slot);
            long nowMillis = new DateTime().getMillis();
            stamp.setTimestamp(millis);
            // Only if we are managing the shard and rolling it up, we
            // should emit a metric here, otherwise, it will be emitted by
            // the rollup context which is responsible for rolling up the
            // shard
            if (parent.getManagedShards().contains(shard) && Configuration.getInstance().getBooleanProperty(CoreConfig.ROLLUP_MODE)) {
                if (stamp.getState().equals(UpdateStamp.State.Rolled) && granularity.snapMillis(stamp.getTimestamp()) == granularity.snapMillis(millis)) {
                    granToReRollMeters.get(granularity).mark();
                    if (nowMillis - millis >= millisInADay) {
                        granToDelayedMetricsMeter.get(granularity).mark();
                    }
                }
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

    protected List<Integer> getSlotsEligibleForRollup(long now, long maxAgeMillis, long delayedMetricsMaxAgeMillis) {
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

            if (update.getLastRollupTimestamp() > 0) {
                final long timeElapsedSinceLastRollup = now - update.getLastRollupTimestamp();

                //Handling re-rolls: Re-rolls would use different delay
                //Since we only allow delayed metrics upto 3 days(BEFORE_CURRENT_COLLECTIONTIME_MS), a slot can be
                //identified as being re-rolled, if the last rollup is within those last 3 days. (theoretically, if
                //there are no delayed metrics, a slot should only get rolled again after 14 days since its last rollup)
                if (timeElapsedSinceLastRollup < DELAYED_METRICS_MAX_ALLOWED_DELAY &&
                        timeElapsed <= delayedMetricsMaxAgeMillis) {
                    log.debug(String.format("Delaying rollup for shard:[%s], slotState:(%s,%s,%s) as [%d] millis " +
                                    "havent elapsed since last rollup:[%d]", shard, granularity, entry.getKey(),
                            update.getState().code(), delayedMetricsMaxAgeMillis, update.getLastRollupTimestamp()));
                    continue;
                }
            }

            outputKeys.add(entry.getKey());
        }
        return outputKeys;
    }
}
