package com.rackspacecloud.blueflood.service;

import com.google.common.base.Ticker;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.utils.Clock;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;


public class ShardStateManagerTest {

    private final int TEST_SHARD = 0;
    private final int TEST_SLOT = 0;
    private final Granularity TEST_GRANULARITY = Granularity.MIN_5;

    private List<Integer> managedShards = new ArrayList<Integer>() {{ add(TEST_SHARD); }};

    private ShardStateManager.SlotStateManager slotStateManager;

    private final Clock mockClock = Mockito.mock(Clock.class);
    private final long lastIngestTime = 1235L;

    @Before
    public void setup() {
        ShardStateManager shardStateManager = new ShardStateManager(managedShards, Ticker.systemTicker(), mockClock);
        slotStateManager = shardStateManager.getSlotStateManager(TEST_SHARD, TEST_GRANULARITY);

        when(mockClock.now()).thenReturn(new Instant(lastIngestTime));
    }

    @Test
    public void testUpdateSlotsOnReadForSlotsNeverRolled() {
        //during startup
        //This tests -> if (stamp == null)

        final long lastCollectionTime = System.currentTimeMillis();
        final long activeSlotLastUpdatedTime = lastCollectionTime + 10; //to make it different from lastCollectionTime

        List<SlotState> slotStates = new ArrayList<SlotState>() {{
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Active, lastCollectionTime, activeSlotLastUpdatedTime));
        }};

        for (SlotState slotState: slotStates) {
            slotStateManager.updateSlotOnRead(slotState);
        }

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Active, updateStamp.getState());
        assertEquals("Invalid last collection timestamp", lastCollectionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup time should not be set", 0, updateStamp.getLastRollupTimestamp());
        assertEquals("Invalid last ingest timestamp", activeSlotLastUpdatedTime, updateStamp.getLastIngestTimestamp());
    }

    @Test
    public void testUpdateSlotsOnReadForRolledSlots() {
        //during startup
        //This tests -> if (stamp.getTimestamp() == timestamp && state.equals(UpdateStamp.State.Rolled))

        final long lastCollectionTime = System.currentTimeMillis() - 10 * 60 * 1000; //minus 10 mins
        final long rolledSlotLastUpdatedTime = System.currentTimeMillis() - 5 * 60 * 1000;    //minus 5 mins
        final long activeSlotLastUpdatedTime = lastCollectionTime + 10; //to make it different from lastCollectionTime

        //Both active and rolled states have same last collection timestamp
        List<SlotState> slotStates = new ArrayList<SlotState>() {{
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Active, lastCollectionTime, activeSlotLastUpdatedTime));
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Rolled, lastCollectionTime, rolledSlotLastUpdatedTime));
        }};

        for (SlotState slotState: slotStates) {
            slotStateManager.updateSlotOnRead(slotState);
        }

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Rolled, updateStamp.getState());
        assertEquals("Last collection timestamp is incorrect", lastCollectionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", rolledSlotLastUpdatedTime, updateStamp.getLastRollupTimestamp());
        assertEquals("Last ingest timestamp is incorrect", activeSlotLastUpdatedTime, updateStamp.getLastIngestTimestamp());
    }

    @Test
    public void testUpdateSlotsOnReadForRolledSlotsButGotDelayedMetrics() {
        //during startup
        //This tests -> if (state.equals(UpdateStamp.State.Rolled))

        final long lastCollectionTime = System.currentTimeMillis() - 10 * 60 * 1000;  //minus 10 mins
        final long rolledSlotLastUpdatedTime =  System.currentTimeMillis() - 5 * 60 * 1000;    //minus 5 mins

        final long delayedMetricCollectionTime = lastCollectionTime - 1; //it just has to be different than lastCollectionTime
        final long activeSlotLastUpdatedTime = System.currentTimeMillis(); //means we ingested delayed metric recently

        //Both active and rolled states have different last collection timestamp.
        //The slot last update time is also different as we got a delayed metric later and "Active" got updated cos of that.
        List<SlotState> slotStates = new ArrayList<SlotState>() {{
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Active, delayedMetricCollectionTime, activeSlotLastUpdatedTime));
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Rolled, lastCollectionTime, rolledSlotLastUpdatedTime));
        }};

        for (SlotState slotState: slotStates) {
            slotStateManager.updateSlotOnRead(slotState);
        }

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Active, updateStamp.getState());
        assertEquals("Last collection timestamp is incorrect", delayedMetricCollectionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", rolledSlotLastUpdatedTime, updateStamp.getLastRollupTimestamp());
        assertEquals("Last ingest timestamp is incorrect", activeSlotLastUpdatedTime, updateStamp.getLastIngestTimestamp());
    }

    private void establishCurrentState() {
        final long existingCollectionTime = System.currentTimeMillis() - 60 * 1000;                  //minus 1 min
        final long lastRolledCollectionTime = existingCollectionTime - 14 * 24 * 60 * 60 * 1000;      //minus 14 days
        final long rolledSlotLastUpdatedTime =  lastRolledCollectionTime + 5 * 60 * 1000;
        final long activeSlotLastUpdatedTime = existingCollectionTime + 10; //to make it different from lastCollectionTime

        List<SlotState> slotStates = new ArrayList<SlotState>() {{
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Active, existingCollectionTime, activeSlotLastUpdatedTime));
            add(createSlotState(TEST_GRANULARITY, UpdateStamp.State.Rolled, lastRolledCollectionTime, rolledSlotLastUpdatedTime));
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
        final long lastRollupTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getLastRollupTimestamp();

        final long lastCollectionTime = System.currentTimeMillis();
        final long lastUpdatedTime = System.currentTimeMillis();
        SlotState newUpdateForActiveSlot = createSlotState(Granularity.MIN_5, UpdateStamp.State.Active, lastCollectionTime, lastUpdatedTime);

        //new incoming slot state
        slotStateManager.updateSlotOnRead(newUpdateForActiveSlot);

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Active, updateStamp.getState());
        assertEquals("Last collection timestamp is incorrect", lastCollectionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", lastRollupTime, updateStamp.getLastRollupTimestamp());
        assertEquals("Last ingest timestamp is incorrect", lastUpdatedTime, updateStamp.getLastIngestTimestamp());
    }

    @Test
    public void testUpdateSlotsOnReadWithIncomingActiveStateButOlderData() {
        //updating existing in-memory map (current state: active, incoming: active state but with old collection timestamp)
        //This tests -> if (stamp.getTimestamp() != timestamp && state.equals(UpdateStamp.State.Active))
        //This tests -> else part of if (!(stamp.getState().equals(UpdateStamp.State.Active) && (stamp.getTimestamp() > timestamp || stamp.isDirty())))

        establishCurrentState();
        final long lastRollupTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getLastRollupTimestamp();
        final long existingLastCollectionTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getTimestamp();
        final long existingLastIngestTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getLastIngestTimestamp();

        long lastCollectionTime = existingLastCollectionTime - 60 * 1000;    //minus 1 min
        long lastUpdatedTime = System.currentTimeMillis();
        SlotState newUpdateForActiveSlot = createSlotState(Granularity.MIN_5, UpdateStamp.State.Active, lastCollectionTime, lastUpdatedTime);

        //new incoming slot state
        slotStateManager.updateSlotOnRead(newUpdateForActiveSlot);

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Active, updateStamp.getState());
        assertEquals("Last collection timestamp is incorrect", existingLastCollectionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", lastRollupTime, updateStamp.getLastRollupTimestamp());
        assertEquals("Dirty flag is incorrect", true, updateStamp.isDirty());
        assertEquals("Last ingest timestamp is incorrect", existingLastIngestTime, updateStamp.getLastIngestTimestamp());
    }

    @Test
    public void testUpdateSlotsOnReadWithIncomingActiveStateButInMemoryDirtyData() {
        //updating existing in-memory map (current state: active with dirty data, incoming: active state)
        //This tests -> if (stamp.getTimestamp() != timestamp && state.equals(UpdateStamp.State.Active))
        //This tests -> else part of if (!(stamp.getState().equals(UpdateStamp.State.Active) && (stamp.getTimestamp() > timestamp || stamp.isDirty())))

        establishCurrentState();
        final long lastRollupTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getLastRollupTimestamp();
        final long existingLastCollectionTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getTimestamp();
        final long existingLastIngestTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getLastIngestTimestamp();
        slotStateManager.getSlotStamps().get(TEST_SLOT).setDirty(true);

        long lastCollectionTime = existingLastCollectionTime + 60 * 1000;    //not older data
        long lastUpdatedTime = System.currentTimeMillis();
        SlotState newUpdateForActiveSlot = createSlotState(Granularity.MIN_5, UpdateStamp.State.Active, lastCollectionTime, lastUpdatedTime);

        //new incoming slot state
        slotStateManager.updateSlotOnRead(newUpdateForActiveSlot);

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Active, updateStamp.getState());
        assertEquals("Last collection timestamp is incorrect", existingLastCollectionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", lastRollupTime, updateStamp.getLastRollupTimestamp());
        assertEquals("Dirty flag is incorrect", true, updateStamp.isDirty());
        assertEquals("Last ingest timestamp is incorrect", existingLastIngestTime, updateStamp.getLastIngestTimestamp());
    }

    @Test
    public void testUpdateSlotsOnReadIncomingRolledStateSameTimestamp() {
        //updating existing in-memory map (current state:active, incoming: rolled state with same collection timestamp)
        //This tests -> if (stamp.getTimestamp() == timestamp && state.equals(UpdateStamp.State.Rolled))

        establishCurrentState();
        final long existingLastCollectionTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getTimestamp();
        final long existingLastIngestTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getLastIngestTimestamp();

        final long newRolledSlotLastUpdatedTime = System.currentTimeMillis();
        SlotState newUpdateForActiveSlot = createSlotState(Granularity.MIN_5, UpdateStamp.State.Rolled, existingLastCollectionTime, newRolledSlotLastUpdatedTime);

        //new incoming slot state
        slotStateManager.updateSlotOnRead(newUpdateForActiveSlot);

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Rolled, updateStamp.getState());
        assertEquals("Last collection timestamp is incorrect", existingLastCollectionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", newRolledSlotLastUpdatedTime, updateStamp.getLastRollupTimestamp());
        assertEquals("Last ingest timestamp is incorrect", existingLastIngestTime, updateStamp.getLastIngestTimestamp());
    }

    @Test
    public void testUpdateSlotsOnReadIncomingRolledStateDifferentTimestamp() {
        //updating existing in-memory map (current state: active, incoming: rolled state with different collection timestamp)
        //This tests -> if (state.equals(UpdateStamp.State.Rolled))

        establishCurrentState();
        final long existingLastCollectionTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getTimestamp();
        final long existingLastIngestTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getLastIngestTimestamp();

        final long newLastRolledCollectionTime = existingLastCollectionTime - 1; //making it different from the collection time we already have in "Active"
        final long newRolledSlotLastUpdatedTime = System.currentTimeMillis();
        SlotState newUpdateForActiveSlot = createSlotState(Granularity.MIN_5, UpdateStamp.State.Rolled, newLastRolledCollectionTime, newRolledSlotLastUpdatedTime);

        //new incoming slot state
        slotStateManager.updateSlotOnRead(newUpdateForActiveSlot);

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Active, updateStamp.getState());
        assertEquals("Last collection timestamp is incorrect", existingLastCollectionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", newRolledSlotLastUpdatedTime, updateStamp.getLastRollupTimestamp());
        assertEquals("Last ingest timestamp is incorrect", existingLastIngestTime, updateStamp.getLastIngestTimestamp());
    }

    @Test
    public void testUpdateSlotsOnReadIncomingOldRolledState() {
        //updating existing in-memory map (current state: active, incoming: old rolled state;
        //This tests -> if (state.equals(UpdateStamp.State.Rolled))
        // a rolled up state gets marked as "active" because of incoming delayed metric before "rolled" state is saved in db.
        // So rolled state from db will have old lastRolluptimestamp

        establishCurrentState();
        final long existingLastCollectionTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getTimestamp();
        final long existingLastRollupTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getLastRollupTimestamp();
        final long existingLastIngestTime = slotStateManager.getSlotStamps().get(TEST_SLOT).getLastIngestTimestamp();

        final long oldLastRolledCollectionTime = existingLastCollectionTime - 1; //making it different from the collection time we already have in "Active"
        final long oldRolledSlotLastUpdatedTime = System.currentTimeMillis() - 14 * 24 * 60 * 60 * 1000; //minus 14 days
        SlotState newUpdateForActiveSlot = createSlotState(Granularity.MIN_5, UpdateStamp.State.Rolled, oldLastRolledCollectionTime, oldRolledSlotLastUpdatedTime);

        //new incoming slot state
        slotStateManager.updateSlotOnRead(newUpdateForActiveSlot);

        Map<Integer, UpdateStamp> slotStamps = slotStateManager.getSlotStamps();
        UpdateStamp updateStamp = slotStamps.get(TEST_SLOT);

        assertEquals("Unexpected state",  UpdateStamp.State.Active, updateStamp.getState());
        assertEquals("Last collection timestamp is incorrect", existingLastCollectionTime, updateStamp.getTimestamp());
        assertEquals("Last rollup timestamp is incorrect", existingLastRollupTime, updateStamp.getLastRollupTimestamp());
        assertEquals("Last ingest timestamp is incorrect", existingLastIngestTime, updateStamp.getLastIngestTimestamp());
    }

    private SlotState createSlotState(Granularity granularity, UpdateStamp.State state, long lastCollectionTimeStamp, long lastUpdatedTimestamp) {
        return new SlotState(granularity, TEST_SLOT, state).withTimestamp(lastCollectionTimeStamp).withLastUpdatedTimestamp(lastUpdatedTimestamp);
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
        assertEquals("Last ingest time should be set", lastIngestTime, stamp.getLastIngestTimestamp());
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
        assertEquals(lastIngestTime, _stamp.getLastIngestTimestamp());

        // when
        final long lastIngestTime2 = 1237L;
        when(mockClock.now()).thenReturn(new Instant(lastIngestTime2));
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1236L);

        // then
        assertTrue("The slot should still be present in the map", slotStateManager.getSlotStamps().containsKey(0));
        UpdateStamp stamp = slotStateManager.getSlotStamps().get(0);
        assertEquals("The timestamp should have changed", 1236L, _stamp.getTimestamp());
        assertTrue("The slot should be marked dirty", stamp.isDirty());
        assertEquals("The state should be Active", UpdateStamp.State.Active, stamp.getState());
        assertEquals("The last rollup timestamp should be uninitialized", 0, stamp.getLastRollupTimestamp());
        assertEquals("Last ingest time should be set", lastIngestTime2, stamp.getLastIngestTimestamp());
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
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(0, 0, 0, 3600000);

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
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(0, 0, 0, 3600000);

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
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(1235L, 30000, 0, 3600000);

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
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(2346L, 0, 70000, 3600000);

        // then
        assertNotNull(slots);
        assertTrue("No slots should be returned", slots.isEmpty());
    }

    @Test
    public void getSlotsEligibleReturnsSlotsThatWereRolledRecentlyButReadyForReroll() {

        // given
        when(mockClock.now()).thenReturn(new Instant(2234L)); //ingesting delayed metric after 1000ms (SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS = 2000ms)
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        UpdateStamp stamp = slotStateManager.getSlotStamps().get(0);
        stamp.setLastRollupTimestamp(2345L);

        // when
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(3235L, 0, 2000, 3600000);

        // then
        assertNotNull(slots);
        assertEquals("Only one slot should be returned", 1, slots.size());
        assertEquals("Slot zero should be included", 0, slots.get(0).intValue());
    }

    @Test
    public void getSlotsEligibleReturnsSlotsThatWereRolledAndGotMetricsWithLongerDelay() {

        // given
        when(mockClock.now()).thenReturn(new Instant(4234L)); //ingesting delayed metric after 3000ms (SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS = 2000ms)
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        UpdateStamp stamp = slotStateManager.getSlotStamps().get(0);
        stamp.setLastRollupTimestamp(2345L);

        // when
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(4235L, 0, 2000, 1000);

        // then
        // then
        assertNotNull(slots);
        assertTrue("No slots should be returned", slots.isEmpty());
    }

    @Test
    public void getSlotsEligibleReturnsSlotsThatWereRolledAndGotMetricsWithLongerDelayButReadyToReroll() {

        // given
        when(mockClock.now()).thenReturn(new Instant(4234L)); //ingesting delayed metric after 3000ms (SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS = 2000ms)
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        UpdateStamp stamp = slotStateManager.getSlotStamps().get(0);
        stamp.setLastRollupTimestamp(2345L);

        // when
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(5235L, 0, 2000, 1000);


        // then
        assertNotNull(slots);
        assertEquals("Only one slot should be returned", 1, slots.size());
        assertEquals("Slot zero should be included", 0, slots.get(0).intValue());
    }

    @Test
    public void getSlotsEligibleReturnsSlotsThatWereRolledAndGotMetricsWithRepeatedLongerDelayButReadyToReroll() {

        // given
        when(mockClock.now()).thenReturn(new Instant(4234L)); //ingesting delayed metric after 3000ms (SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS = 2000ms)
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        UpdateStamp stamp = slotStateManager.getSlotStamps().get(0);
        stamp.setLastRollupTimestamp(2345L);

        //pre condition - 1
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(4235L, 0, 2000, 1000);
        assertTrue("No slots should be returned", slots.isEmpty());

        //pre condition - 2
        when(mockClock.now()).thenReturn(new Instant(5234L)); //ingesting delayed metric after 4000ms, right before ROLLUP_WAIT(1000ms) elapses
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);
        List<Integer> slots1 = slotStateManager.getSlotsEligibleForRollup(5235L, 0, 2000, 1000);
        assertTrue("No slots should be returned", slots1.isEmpty());

        //when
        List<Integer> slots2 = slotStateManager.getSlotsEligibleForRollup(6235L, 0, 2000, 1000);

        // then
        assertNotNull(slots);
        assertEquals("Only one slot should be returned", 1, slots2.size());
        assertEquals("Slot zero should be included", 0, slots2.get(0).intValue());
    }

    @Test
    public void getSlotsEligibleReturnsSlotsThatWereNotRolled() {

        // given
        slotStateManager.createOrUpdateForSlotAndMillisecond(0, 1234L);

        // when
        List<Integer> slots = slotStateManager.getSlotsEligibleForRollup(2346L, 0, 1, 3600000);

        // then
        assertNotNull(slots);
        assertEquals("Only one slot should be returned", 1, slots.size());
        assertEquals("Slot zero should be included", 0, slots.get(0).intValue());
    }
}
