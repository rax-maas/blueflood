package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.rollup.SlotKey;

/**
 * Interface abstracting out the slot checks that are currently queued.
 *
 * @author Jeeyoung Kim
 */
public interface SlotCheckScheduler {
    /**
     * Returns the next slot check to run.
     *
     * Side effects:
     * <ul>
     *   <li>resets update tracking for that slot.</li>
     *   <li>adds the key to the set of running rollups.</li>
     * </ul>
     * <b>If this method returns non-null object, you MUST call {@link #markFinished(SlotKey)} or {@link #reschedule(SlotKey, boolean)} afterwards.</b>
     *
     * @return {@link SlotKey} representing
     */
    public SlotKey getNextScheduled();

    /**
     * Mark the given slot check as finished.
     * @param slotKey slot key in interest.
     */
    void markFinished(SlotKey slotKey);

    /**
     * Re-schedule the given slot check.
     * @param slotKey slot key in interest.
     * @param rescheduleImmediately true if the given slot check should be rescheduled immediately.
     */
    void reschedule(SlotKey slotKey, boolean rescheduleImmediately);
}
