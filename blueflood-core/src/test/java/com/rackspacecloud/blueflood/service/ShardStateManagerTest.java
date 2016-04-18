package com.rackspacecloud.blueflood.service;

import com.google.common.base.Ticker;
import com.rackspacecloud.blueflood.rollup.Granularity;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;


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

    @Test
    public void testUpdateSlotsOnReadIncomingOldRolledState() {
        //updating existing in-memory map (current state: active, incoming: old rolled state;
        //This tests -> if (state.equals(UpdateStamp.State.Rolled))
        // a rolled up state gets marked as "active" because of incoming delayed metric before "rolled" state is saved in db.
        // So rolled state from db will have old lastRolluptimestamp

        establishCurrentState();
        long existingLastIngestionTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getTimestamp();
        long existingLastRollupTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getLastRollupTimestamp();

        long oldLastRolledIngestionTime = existingLastIngestionTime - 1; //making it different from the ingest time we already have in "Active"
        long oldRolledSlotLastUpdatedTime = System.currentTimeMillis() - 14 * 24 * 60 * 60 * 1000; //minus 14 days
        SlotState newUpdateForActiveSlot = createSlotState(Granularity.MIN_5, UpdateStamp.State.Rolled, oldLastRolledIngestionTime, oldRolledSlotLastUpdatedTime);

        //new incoming slot state
        slotStateManager.updateSlotOnRead(newUpdateForActiveSlot);

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Active, updateStamp.getState());
        assertEquals("Last ingestion timestamp is incorrect", existingLastIngestionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", existingLastRollupTime, updateStamp.getLastRollupTimestamp());
    }

    private SlotState createSlotState(Granularity granularity, UpdateStamp.State state, long lastIngestionTimeStamp, long lastUpdatedTimestamp) {
        return new SlotState(granularity, TEST_SLOT, state).withTimestamp(lastIngestionTimeStamp).withLastUpdatedTimestamp(lastUpdatedTimestamp);
    }

    @Test
    public void createOrUpdateCreatesWhenSlotNotPresent() {

        // precondition
        assertFalse(slotStateManager.getSlotStamps().containsKey(0));

        // when
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);

        // then
        assertTrue("The slot should be present in the map", slotStateManager.getSlotStamps().containsKey(0));
        UpdateStamp stamp = slotStateManager.getSlotStamps().get(0);
        assertTrue("The slot should be marked dirty", stamp.isDirty());
        assertEquals("The timestamp should be set", 1234L, stamp.getTimestamp());
        assertEquals("The state should be Active", UpdateStamp.State.Active, stamp.getState());
        assertEquals("The last rollup timestamp should be uninitialized", 0, stamp.getLastRollupTimestamp());
    }

    @Test
    public void createOrUpdateUpdatesWhenSlotAlreadyPresent() {

        // given
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        UpdateStamp _stamp = slotStateManager.getSlotStamps().get(0);
        _stamp.setDirty(false);
        _stamp.setState(UpdateStamp.State.Rolled);

        // precondition
        assertEquals(1234L, _stamp.getTimestamp());
        assertFalse(_stamp.isDirty());
        assertEquals(UpdateStamp.State.Rolled, _stamp.getState());
        assertEquals(0, _stamp.getLastRollupTimestamp());

        // when
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1235L);

        // then
        assertTrue("The slot should still be present in the map", slotStateManager.getSlotStamps().containsKey(0));
        UpdateStamp stamp = slotStateManager.getSlotStamps().get(0);
        assertEquals("The timestamp should have changed", 1235L, _stamp.getTimestamp());
        assertTrue("The slot should be marked dirty", stamp.isDirty());
        assertEquals("The state should be Active", UpdateStamp.State.Active, stamp.getState());
        assertEquals("The last rollup timestamp should be uninitialized", 0, stamp.getLastRollupTimestamp());
    }

    @Test
    public void getDirtySlotsWhenEmptyReturnsEmpty() {

        // precondition
        assertTrue(slotStateManager.getSlotStamps().isEmpty());

        // when
        Map dirtySlots = slotStateManager.getDirtySlotStampsAndMarkClean();

        // then
        assertNotNull(dirtySlots);
        assertTrue("No slots should be included", dirtySlots.isEmpty());
    }

    @Test
    public void getDirtySlotsWhenContainsOnlyCleanSlotsReturnsEmpty() {

        // given
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        UpdateStamp _stamp = slotStateManager.getSlotStamps().get(0);
        _stamp.setDirty(false);

        // when
        Map dirtySlots = slotStateManager.getDirtySlotStampsAndMarkClean();

        // then
        assertNotNull(dirtySlots);
        assertTrue("No slots should be included", dirtySlots.isEmpty());
    }

    @Test
    public void getDirtySlotsWhenContainsOnlyDirtySlotsReturnsThoseSlotsAndMarksClean() {

        // given
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        slotStateManager.createOrUpdateForSlotAndMillisecond(1, 1234L);

        // when
        Map<Integer, UpdateStamp> dirtySlots = slotStateManager.getDirtySlotStampsAndMarkClean();

        // then
        assertNotNull(dirtySlots);
        assertEquals("Both slots should be returned", 2, dirtySlots.size());

        assertTrue("The first slot should be included", dirtySlots.containsKey(0));
        UpdateStamp stamp = dirtySlots.get(0);
        assertFalse("The first slot should be clean", stamp.isDirty());

        assertTrue("The second slot should be included", dirtySlots.containsKey(1));
        stamp = dirtySlots.get(1);
        assertFalse("The second slot should be clean", stamp.isDirty());
    }

    @Test
    public void getDirtySlotsWhenContainsCleanAndDirtySlotsReturnsOnlyDirtySlotsAndMarksClean() {

        // given
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        slotStateManager.getSlotStamps().get(0).setDirty(false);
        slotStateManager.createOrUpdateForSlotAndMillisecond(1, 1234L);

        // when
        Map<Integer, UpdateStamp> dirtySlots = slotStateManager.getDirtySlotStampsAndMarkClean();

        // then
        assertNotNull(dirtySlots);
        assertEquals("Only one slot should be returned", 1, dirtySlots.size());

        assertFalse("Slot 0 should not be included", dirtySlots.containsKey(0));
        assertFalse("Slot 0 should still be clean", slotStateManager.getSlotStamps().get(0).isDirty());

        assertTrue("Slot 1 should be included", dirtySlots.containsKey(1));
        assertFalse("Slot 1 should now be clean", dirtySlots.get(1).isDirty());
    }

    @Test
    public void getAndSetStateGetsAndSetsState() {

        // given
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);

        // precondition
        assertEquals(UpdateStamp.State.Active, slotStateManager.getSlotStamps().get(0).getState());

        // when
        UpdateStamp stamp = slotStateManager.getAndSetState(0, UpdateStamp.State.Rolled);

        // then
        assertNotNull(stamp);
        assertSame("The stamp returned should be the same one for slot 0", slotStateManager.getSlotStamps().get(0), stamp);
        assertEquals("The state should be changed to Rolled", UpdateStamp.State.Rolled, stamp.getState());
    }

    @Test
    public void getAndSetStateDoesNotAffectUnspecifiedSlot() {

        // given
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        slotStateManager.createOrUpdateForSlotAndMillisecond(1, 1235L);

        // precondition
        assertEquals(UpdateStamp.State.Active, slotStateManager.getSlotStamps().get(0).getState());
        assertEquals(UpdateStamp.State.Active, slotStateManager.getSlotStamps().get(1).getState());

        // when
        UpdateStamp stamp = slotStateManager.getAndSetState(0, UpdateStamp.State.Rolled);

        // then
        assertNotNull(stamp);
        assertSame(slotStateManager.getSlotStamps().get(0), stamp);
        assertEquals("Slot 0 should now be Rolled", UpdateStamp.State.Rolled, stamp.getState());
        assertEquals("Slot 0 should still be Active", UpdateStamp.State.Active, slotStateManager.getSlotStamps().get(1).getState());
    }

    @Test(expected = NullPointerException.class)
    public void getAndSetStateUninitializedSlotThrowsException() {

        // precondition
        assertEquals(0, slotStateManager.getSlotStamps().size());

        // when
        UpdateStamp stamp = slotStateManager.getAndSetState(0, UpdateStamp.State.Rolled);

        // then
        // the exception is thrown
    }

    @Test
    public void getSlotsEligibleUninitializedReturnsEmpty() {

        // precondition
        assertEquals(0, slotStateManager.getSlotStamps().size());

        // when
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(0, 0, 0);

        // then
        assertNotNull(slots);
        assertTrue("No slots should be returned", slots.isEmpty());
    }

    @Test
    public void getSlotsEligibleDoesNotReturnRolledSlots() {

        // given
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        slotStateManager.getAndSetState(0, UpdateStamp.State.Rolled);

        // when
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(0, 0, 0);

        // then
        assertNotNull(slots);
        assertTrue("No slots should be returned", slots.isEmpty());
    }

    @Test
    public void getSlotsEligibleDoesNotReturnSlotsThatAreTooYoung() {

        // given
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        slotStateManager.getAndSetState(0, UpdateStamp.State.Active);

        // when
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(1235L, 30000, 0);

        // then
        assertNotNull(slots);
        assertTrue("No slots should be returned", slots.isEmpty());
    }

    @Test
    public void getSlotsEligibleDoesNotReturnSlotsThatWereRolledRecently() {

        // given
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        UpdateStamp stamp = slotStateManager.getSlotStamps().get(0);
        stamp.setLastRollupTimestamp(2345L);

        // when
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(2346L, 0, 70000);

        // then
        assertNotNull(slots);
        assertTrue("No slots should be returned", slots.isEmpty());
    }

    @Test
    public void getSlotsEligibleReturnsSlotsThatWereRolledButNotRecently() {

        // given
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        UpdateStamp stamp = slotStateManager.getSlotStamps().get(0);
        stamp.setLastRollupTimestamp(2345L);

        // when
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(2346L, 0, 1);

        // then
        assertNotNull(slots);
        assertEquals("Only one slot should be returned", 1, slots.size());
        assertEquals("Slot zero should be included", 0, slots.get(0).intValue());
    }

    @Test
    public void getSlotsEligibleReturnsSlotsThatWereNotRolled() {

        // given
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);

        // when
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(2346L, 0, 1);

        // then
        assertNotNull(slots);
        assertEquals("Only one slot should be returned", 1, slots.size());
        assertEquals("Slot zero should be included", 0, slots.get(0).intValue());
    }
}
