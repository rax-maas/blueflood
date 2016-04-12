package com.rackspacecloud.blueflood.service;

import com.google.common.base.Ticker;
import com.rackspacecloud.blueflood.rollup.Granularity;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class ShardStateManagerTest {

    private final int TEST_SHARD = 0;
    private final int TEST_SLOT = 0;
    private final Granularity TEST_GRANULARITY = Granularity.MIN_5;

    private List<Integer> managedShards = new ArrayList<Integer>() {{ add(TEST_SHARD); }};

    private ShardStateManager.SlotStateManager slotStateManager;


    @Before
    public void setup() {
        ShardStateManager shardStateManager = new ShardStateManager(managedShards, Ticker.systemTicker());
        slotStateManager = shardStateManager.getSlotStateManager(TEST_SHARD, TEST_GRANULARITY);
    }

    @Test
    public void testUpdateSlotsOnReadForSlotsNeverRolled() {
        //during startup
        //This tests -> if (stamp == null)

        final long lastIngestionTime = System.currentTimeMillis();

        List<SlotState> slotStates = new ArrayList<SlotState>() {{
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Active, lastIngestionTime, lastIngestionTime));
        }};

        for (SlotState slotState: slotStates) {
            slotStateManager.updateSlotOnRead(slotState);
        }

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Active, updateStamp.getState());
        assertEquals("Invalid last ingestion timestamp", lastIngestionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup time should not be set", 0, updateStamp.getLastRollupTimestamp());
    }

    @Test
    public void testUpdateSlotsOnReadForRolledSlots() {
        //during startup
        //This tests -> if (stamp.getTimestamp() == timestamp && state.equals(UpdateStamp.State.Rolled))

        final long lastIngestionTime = System.currentTimeMillis() - 10 * 60 * 1000; //minus 10 mins
        final long rolledSlotLastUpdatedTime = System.currentTimeMillis() - 5 * 60 * 1000;    //minus 5 mins

        //Both active and rolled states have same last ingested timestamp
        List<SlotState> slotStates = new ArrayList<SlotState>() {{
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Active, lastIngestionTime, lastIngestionTime));
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Rolled, lastIngestionTime, rolledSlotLastUpdatedTime));
        }};

        for (SlotState slotState: slotStates) {
            slotStateManager.updateSlotOnRead(slotState);
        }

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Rolled, updateStamp.getState());
        assertEquals("Last ingestion timestamp is incorrect", lastIngestionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", rolledSlotLastUpdatedTime, updateStamp.getLastRollupTimestamp());
    }

    @Test
    public void testUpdateSlotsOnReadForRolledSlotsButGotDelayedMetrics() {
        //during startup
        //This tests -> if (state.equals(UpdateStamp.State.Rolled))

        final long lastIngestionTime = System.currentTimeMillis() - 10 * 60 * 1000;  //minus 10 mins
        final long rolledSlotLastUpdatedTime =  System.currentTimeMillis() - 5 * 60 * 1000;    //minus 5 mins

        final long delayedMetricIngestionTime = lastIngestionTime - 1; //it just has to be different than lastIngestionTime
        final long activeSlotLastUpdatedTime = System.currentTimeMillis(); //means we ingested delayed metric recently

        //Both active and rolled states have different last ingested timestamp.
        //The slot last update time is also different as we got a delayed metric later and "Active" got updated cos of that.
        List<SlotState> slotStates = new ArrayList<SlotState>() {{
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Active, delayedMetricIngestionTime, activeSlotLastUpdatedTime));
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Rolled, lastIngestionTime, rolledSlotLastUpdatedTime));
        }};

        for (SlotState slotState: slotStates) {
            slotStateManager.updateSlotOnRead(slotState);
        }

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Active, updateStamp.getState());
        assertEquals("Last ingestion timestamp is incorrect", delayedMetricIngestionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", rolledSlotLastUpdatedTime, updateStamp.getLastRollupTimestamp());
    }

    private void establishCurrentState() {
        final long existingIngestionTime = System.currentTimeMillis() - 60 * 1000;                  //minus 1 min
        final long lastRolledIngestionTime = existingIngestionTime - 14 * 24 * 60 * 60 * 1000;      //minus 14 days
        final long rolledSlotLastUpdatedTime =  lastRolledIngestionTime + 5 * 60 * 1000;

        List<SlotState> slotStates = new ArrayList<SlotState>() {{
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Active, existingIngestionTime, existingIngestionTime));
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Rolled, lastRolledIngestionTime, rolledSlotLastUpdatedTime));
        }};

        //establishing state as active (with in-memory current state as Active)
        for (SlotState slotState: slotStates) {
            slotStateManager.updateSlotOnRead(slotState);
        }
    }

    @Test
    public void testUpdateSlotsOnReadWithIncomingActiveState() {
        //updating existing in-memory map (current state: active, incoming: active state)
        //This tests -> if (stamp.getTimestamp() != timestamp && state.equals(UpdateStamp.State.Active))
        //This tests -> if (!(stamp.getState().equals(UpdateStamp.State.Active) && (stamp.getTimestamp() > timestamp || stamp.isDirty())))

        establishCurrentState();
        long lastRollupTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getLastRollupTimestamp();

        long lastIngestionTime = System.currentTimeMillis();
        long lastUpdatedTime = System.currentTimeMillis();
        SlotState newUpdateForActiveSlot = createSlotState(Granularity.MIN_5, UpdateStamp.State.Active, lastIngestionTime, lastUpdatedTime);

        //new incoming slot state
        slotStateManager.updateSlotOnRead(newUpdateForActiveSlot);

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Active, updateStamp.getState());
        assertEquals("Last ingestion timestamp is incorrect", lastIngestionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", lastRollupTime, updateStamp.getLastRollupTimestamp());
    }

    @Test
    public void testUpdateSlotsOnReadWithIncomingActiveStateButOlderData() {
        //updating existing in-memory map (current state: active, incoming: active state but with old ingest timestamp)
        //This tests -> if (stamp.getTimestamp() != timestamp && state.equals(UpdateStamp.State.Active))
        //This tests -> else part of if (!(stamp.getState().equals(UpdateStamp.State.Active) && (stamp.getTimestamp() > timestamp || stamp.isDirty())))

        establishCurrentState();
        long lastRollupTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getLastRollupTimestamp();
        long existingLastIngestionTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getTimestamp();

        long lastIngestionTime = existingLastIngestionTime - 60 * 1000;    //minus 1 min
        long lastUpdatedTime = System.currentTimeMillis();
        SlotState newUpdateForActiveSlot = createSlotState(Granularity.MIN_5, UpdateStamp.State.Active, lastIngestionTime, lastUpdatedTime);

        //new incoming slot state
        slotStateManager.updateSlotOnRead(newUpdateForActiveSlot);

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Active, updateStamp.getState());
        assertEquals("Last ingestion timestamp is incorrect", existingLastIngestionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", lastRollupTime, updateStamp.getLastRollupTimestamp());
        assertEquals("Dirty flag is incorrect", true, updateStamp.isDirty());
    }

    @Test
    public void testUpdateSlotsOnReadIncomingRolledStateSameTimestamp() {
        //updating existing in-memory map (current state:active, incoming: rolled state with same ingest timestamp)
        //This tests -> if (stamp.getTimestamp() == timestamp && state.equals(UpdateStamp.State.Rolled))

        establishCurrentState();
        long existingLastIngestionTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getTimestamp();

        long newRolledSlotLastUpdatedTime = System.currentTimeMillis();
        SlotState newUpdateForActiveSlot = createSlotState(Granularity.MIN_5, UpdateStamp.State.Rolled, existingLastIngestionTime, newRolledSlotLastUpdatedTime);

        //new incoming slot state
        slotStateManager.updateSlotOnRead(newUpdateForActiveSlot);

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Rolled, updateStamp.getState());
        assertEquals("Last ingestion timestamp is incorrect", existingLastIngestionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", newRolledSlotLastUpdatedTime, updateStamp.getLastRollupTimestamp());
    }

    @Test
    public void testUpdateSlotsOnReadIncomingRolledStateDifferentTimestamp() {
        //updating existing in-memory map (current state: active, incoming: rolled state with different ingest timestamp)
        //This tests -> if (state.equals(UpdateStamp.State.Rolled))

        establishCurrentState();
        long existingLastIngestionTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getTimestamp();

        long newLastRolledIngestionTime = existingLastIngestionTime - 1; //making it different from the ingest time we already have in "Active"
        long newRolledSlotLastUpdatedTime = System.currentTimeMillis();
        SlotState newUpdateForActiveSlot = createSlotState(Granularity.MIN_5, UpdateStamp.State.Rolled, newLastRolledIngestionTime, newRolledSlotLastUpdatedTime);

        //new incoming slot state
        slotStateManager.updateSlotOnRead(newUpdateForActiveSlot);

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Active, updateStamp.getState());
        assertEquals("Last ingestion timestamp is incorrect", existingLastIngestionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", newRolledSlotLastUpdatedTime, updateStamp.getLastRollupTimestamp());
    }

    private SlotState createSlotState(Granularity granularity, UpdateStamp.State state, long lastIngestionTimeStamp, long lastUpdatedTimestamp) {
        return new SlotState(granularity, TEST_SLOT, state).withTimestamp(lastIngestionTimeStamp).withLastUpdatedTimestamp(lastUpdatedTimestamp);
    }
}
