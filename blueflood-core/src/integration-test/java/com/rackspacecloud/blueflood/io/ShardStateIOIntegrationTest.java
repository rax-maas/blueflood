package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.io.astyanax.AShardStateIO;
import com.rackspacecloud.blueflood.io.datastax.DShardStateIO;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SlotState;
import com.rackspacecloud.blueflood.service.UpdateStamp;
import org.junit.Test;
import static org.junit.Assert.*;


import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by shin4590 on 3/28/16.
 */
public class ShardStateIOIntegrationTest extends IntegrationTestBase {

    private static AShardStateIO astyanaxShardStateIO = new AShardStateIO();
    private static DShardStateIO datastaxMetricsStateIO = new DShardStateIO();

    private static final int NUM_SLOTS = 5;

    @Test
    public void writeAstyanaxReadDatastax() throws Exception {

        // when we write with astyanax
        int shard = RAND.nextInt(127);
        Map<Granularity, Map<Integer, UpdateStamp>> data = generateRandomGranSlotTimestamp();
        astyanaxShardStateIO.putShardState(shard, data);

        // we can read it back with datastax
        Collection<SlotState> slotStates = datastaxMetricsStateIO.getShardState(shard);

        // make sure data is good
        for (SlotState state : slotStates) {
            Map<Integer, UpdateStamp> slot2TsMap = data.get(state.getGranularity());
            assertNotNull("Slot2Timestamp map exists for granularity " + state.getGranularity(), slot2TsMap);
            UpdateStamp ts = slot2TsMap.get(state.getSlot());
            assertNotNull("UpdateStamp exists for slot " + state.getSlot());
            assertTrue("Last update timestamp is retrieved", state.getLastUpdatedTimestamp() > 0);

            assertEquals("State is same for granularity " + state.getGranularity() + ", slot " + state.getSlot(), ts.getState(), state.getState());
        }
    }

    @Test
    public void writeDatastaxReadAstyanax() throws Exception {

        // when we write with datastax
        int shard = RAND.nextInt(127);
        Map<Granularity, Map<Integer, UpdateStamp>> data = generateRandomGranSlotTimestamp();
        datastaxMetricsStateIO.putShardState(shard, data);

        // we can read it back with astyanax
        Collection<SlotState> slotStates = astyanaxShardStateIO.getShardState(shard);

        // make sure data is good
        for (SlotState state : slotStates) {
            Map<Integer, UpdateStamp> slot2TsMap = data.get(state.getGranularity());
            assertNotNull("Slot2Timestamp map exists for granularity " + state.getGranularity(), slot2TsMap);
            UpdateStamp ts = slot2TsMap.get(state.getSlot());
            assertNotNull("UpdateStamp exists for slot " + state.getSlot());
            assertTrue("Last update timestamp is retrieved", state.getLastUpdatedTimestamp() > 0);

            assertEquals("State is same for granularity " + state.getGranularity() + ", slot " + state.getSlot(), ts.getState(), state.getState());
        }

    }

    private Map<Granularity, Map<Integer, UpdateStamp>> generateRandomGranSlotTimestamp() {
        Map<Granularity, Map<Integer, UpdateStamp>> gran2SlotTsMap = new HashMap<Granularity, Map<Integer, UpdateStamp>>();

        Map<Integer, UpdateStamp> slot2TsMap = generateSlotTs();
        gran2SlotTsMap.put(Granularity.FULL, slot2TsMap);

        slot2TsMap = generateSlotTs();
        gran2SlotTsMap.put(Granularity.MIN_5, slot2TsMap);

        slot2TsMap = generateSlotTs();
        gran2SlotTsMap.put(Granularity.MIN_20, slot2TsMap);

        slot2TsMap = generateSlotTs();
        gran2SlotTsMap.put(Granularity.MIN_60, slot2TsMap);

        slot2TsMap = generateSlotTs();
        gran2SlotTsMap.put(Granularity.MIN_240, slot2TsMap);

        slot2TsMap = generateSlotTs();
        gran2SlotTsMap.put(Granularity.MIN_1440, slot2TsMap);

        return gran2SlotTsMap;
    }

    private Map<Integer, UpdateStamp> generateSlotTs() {
        Map<Integer, UpdateStamp> slot2TsMap = new HashMap<Integer, UpdateStamp>();
        // generate data
        for (int count = 0; count<NUM_SLOTS; count++) {
            int slot = RAND.nextInt(127);
            int randomTsDelta = RAND.nextInt(5000);
            UpdateStamp ts = new UpdateStamp(System.currentTimeMillis()-randomTsDelta, UpdateStamp.State.Active, false);
            slot2TsMap.put(Integer.valueOf(slot), ts);
        }
        return slot2TsMap;
    }
}
