package com.cloudkick.blueflood.rollup;

import com.cloudkick.blueflood.inputs.formats.CloudMonitoringTelescope;
import com.cloudkick.blueflood.io.AstyanaxReader;
import com.cloudkick.blueflood.io.AstyanaxWriter;
import com.cloudkick.blueflood.io.CqlTestBase;
import com.cloudkick.blueflood.io.NumericSerializer;
import com.cloudkick.blueflood.service.UpdateStamp;
import com.cloudkick.blueflood.types.Locator;
import com.cloudkick.blueflood.types.Range;
import com.cloudkick.blueflood.types.Rollup;
import com.cloudkick.blueflood.types.ServerMetricLocator;
import com.cloudkick.blueflood.utils.Util;
import com.google.common.collect.Lists;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import telescope.thrift.Metric;
import telescope.thrift.Telescope;
import telescope.thrift.VerificationModel;

import java.io.IOException;
import java.util.*;

/**
 * Some of these tests here were horribly contrived to mimic behavior in Writer. The problem with this approach is that
 * when logic in Writer changes, these tests can break unless the logic is changed here too. */
public class MetricsIntegrationTest extends CqlTestBase {
    
    // returns a collection all checks that were written at some point.
    // this was a lot cooler back when the slot changed with time.
    private Collection<String> writeLocatorsOnly(int hours) throws Exception {
        // duplicate the logic from Writer.writeFull() that inserts locator rows.
        List<String> checkNames = new ArrayList<String>();
        Set<String> allCheckNames = new HashSet<String>();
        int checkNameCounter = 0;
        for (int i = 0; i < hours; i++)
            checkNames.add("test:locator:inserts:" + checkNameCounter++);

        AstyanaxTester at = new AstyanaxTester();
        MutationBatch mb = at.createMutationBatch();
        for (String checkName : checkNames) {
            ServerMetricLocator loc = ServerMetricLocator.createFromTelescopePrimitives(
                    "ac" + CqlTestBase.randString(8), "en" + CqlTestBase.randString(8), checkName, "dim0.intmetric");
            allCheckNames.add(checkName);
            int shard = Util.computeShard(checkName);

            mb.withRow(at.getLocatorCF(), (long)shard)
                    .putColumn(loc, "", 100000);
        }
        mb.execute();
        return allCheckNames;
    }

    private void writeFullData(
            long baseMillis, 
            int hours,
            final String acctId,
            final String entityId,
            final String checkName,
            final String dimension,
            AstyanaxWriter writer) throws Exception {
        // insert something every minute for 48h
        for (int i = 0; i < 60 * hours; i++) {
            final long curMillis = baseMillis + i * 60000;
            final CloudMonitoringTelescope cmTelescope = new CloudMonitoringTelescope(
                    makeTelescope("uuid", checkName, acctId, "module", entityId, "target", curMillis, dimension));

            writer.insertFull(cmTelescope.toMetrics());
        }
    }

    public void testLocatorsWritten() throws Exception {
        Collection<String> checkNames = writeLocatorsOnly(48);

        Set<String> locators = new HashSet<String>();
        AstyanaxReader r = AstyanaxReader.getInstance();
        for (String checkName : checkNames)
            for (Column<Locator> locatorCol : r.getAllLocators(Util.computeShard(checkName)))
                locators.add(locatorCol.getName().toString());
        assertEquals(48, locators.size());
    }

    // tests how rollup generation could be done optimally when regenerating large swaths of rollups.  It 
    // minimizes round trips around column selection.  The operations are reduced to:
    // 1) one long slice read.
    // 2) average calcuations and aggregation (requires memory!)
    // 3) a batch write.
    public void testRollupGenerationOverSwath() throws Exception {
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        AstyanaxReader reader = AstyanaxReader.getInstance();
        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        int hours = 48;
        final String acctId = "ac" + CqlTestBase.randString(8);
        final String entityId = "ac" + CqlTestBase.randString(8);
        final String checkName = "test_rollup_b";
        final String dimension = "dim0";
        final long endMillis = baseMillis + (60 * 60 * hours * 1000);
        writeFullData(baseMillis, hours, acctId, entityId, checkName, dimension, writer);
        final Locator locator = ServerMetricLocator.createFromTelescopePrimitives(acctId, entityId, checkName,
                dimension + ".intmetric");

        for (Granularity gran : new Granularity[] {Granularity.FULL, Granularity.MIN_5, Granularity.MIN_20, Granularity.MIN_60, Granularity.MIN_240}) {
            
            // this logic would have to be encapsulated somewhere:
            // regenerateRollup(long startSecs, long endSecs, String locator, Granularity srcGran);
            // it would need access to a Reader and Writer.
            ArrayList<Range> ranges = new ArrayList<Range>();
            for (Range range : Range.getRangesToRollup(gran, baseMillis, endMillis))
                ranges.add(range);
            Range macroRange = new Range(ranges.get(0).start, ranges.get(ranges.size() - 1).stop);
            Range curRange = ranges.remove(0);
    
            SortedMap<Long, Object> cols = new TreeMap<Long, Object>();
            for (Column<Long> col : reader.getNumericRollups(locator,
                    gran,
                    macroRange.start,
                    macroRange.stop)) {
                cols.put(col.getName(), col.getValue(NumericSerializer.get(gran)));
            }
            Rollup rollup = new Rollup();
            Map<Long, Rollup> rollups = new HashMap<Long, Rollup>();
            for (Map.Entry<Long, Object> col : cols.entrySet()) {
                while (col.getKey() > curRange.stop) {
                    rollups.put(curRange.start, rollup);
                    rollup = new Rollup();
                    curRange = ranges.remove(0);
                }
                if (gran == Granularity.FULL) {
                    Object longOrDouble = col.getValue();
                    rollup.handleFullResMetric(longOrDouble);
                } else {
                    Rollup desRollup = (Rollup)col.getValue();
                    rollup.handleRollupMetric(desRollup);
                }
            }

            rollups.put(curRange.start, rollup);
            writer.insertRollups(locator, rollups, gran.coarser());
        }
        
        // verify the number of points in 48h worth of rollups. 
        Range range = new Range(Granularity.MIN_1440.snapMillis(baseMillis), Granularity.MIN_1440.snapMillis(endMillis + Granularity.MIN_1440.milliseconds()));
        Rollup rollup = reader.readAndCalculate(locator, range, Granularity.MIN_1440);

        assertEquals(60 * hours, rollup.getCount());
    }
    
    public void testRollupGenerationSimple() throws Exception {
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        AstyanaxReader reader = AstyanaxReader.getInstance();
        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        int hours = 48;
        final String acctId = "ac" + CqlTestBase.randString(8);
        final String entityId = "ac" + CqlTestBase.randString(8);
        final String checkName = "test_rollup_a";
        final String dimension = "dim0";
        final long endMillis = baseMillis + (1000 * 60 * 60 * hours);
        writeFullData(baseMillis, hours, acctId, entityId, checkName, dimension, writer);
        final Locator locator = ServerMetricLocator.createFromTelescopePrimitives(acctId, entityId, checkName,
                "dim0.intmetric");

        // FULL -> 5m
        Map<Long, Rollup> rollups = new HashMap<Long, Rollup>();
        for (Range range : Range.getRangesToRollup(Granularity.FULL, baseMillis, endMillis)) {
            // each range should produce one average
            Rollup rollup = reader.readAndCalculate(locator, range, Granularity.FULL);
            rollups.put(range.start, rollup);
        }
        writer.insertRollups(locator, rollups, Granularity.FULL.coarser());

        // 5m -> 20m
        rollups.clear();
        for (Range range : Range.getRangesToRollup(Granularity.MIN_5, baseMillis, endMillis)) {
            Rollup rollup = reader.readAndCalculate(locator, range, Granularity.MIN_5);
            rollups.put(range.start, rollup);
        }

        writer.insertRollups(locator, rollups, Granularity.MIN_5.coarser());

        // 20m -> 60m
        rollups.clear();
        for (Range range : Range.getRangesToRollup(Granularity.MIN_20, baseMillis, endMillis)) {
            Rollup rollup = reader.readAndCalculate(locator, range,
                    Granularity.MIN_20);
            rollups.put(range.start, rollup);
        }
        writer.insertRollups(locator, rollups, Granularity.MIN_20.coarser());

        // 60m -> 240m
        rollups.clear();
        for (Range range : Range.getRangesToRollup(Granularity.MIN_60, baseMillis, endMillis)) {
            Rollup rollup = reader.readAndCalculate(locator, range, Granularity.MIN_60);

            rollups.put(range.start, rollup);
        }
        writer.insertRollups(locator, rollups, Granularity.MIN_60.coarser());

        // 240m -> 1440m
        rollups.clear();
        for (Range range : Range.getRangesToRollup(Granularity.MIN_240, baseMillis, endMillis)) {
            Rollup rollup = reader.readAndCalculate(locator, range, Granularity.MIN_240);
            rollups.put(range.start, rollup);
        }
        writer.insertRollups(locator, rollups, Granularity.MIN_240.coarser());

        // verify the number of points in 48h worth of rollups. 
        Range range = new Range(Granularity.MIN_1440.snapMillis(baseMillis), Granularity.MIN_1440.snapMillis(endMillis + Granularity.MIN_1440.milliseconds()));
        Rollup rollup = reader.readAndCalculate(locator, range, Granularity.MIN_1440);
        assertEquals(60 * hours, rollup.getCount());
    }
    
    public void testMonitoringZoneIsPrepended() throws ConnectionException {
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        AstyanaxReader reader = AstyanaxReader.getInstance();
        String acctId = "ac" + CqlTestBase.randString(8);
        String entityId = "en" + CqlTestBase.randString(8);
        String checkName = "with_mz";
        String mzId = "mzGRD";
        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        final Locator locator = ServerMetricLocator.createFromTelescopePrimitives(acctId, entityId, checkName,
                Util.generateMetricName("intmetric", mzId));
        
        final Telescope withMz = makeTelescope("withMz", checkName, acctId, "module", entityId, "target", baseMillis, null);
        withMz.setMonitoringZoneId(mzId);
        assertTrue(withMz.getMetrics().keySet().contains("intmetric"));
        final CloudMonitoringTelescope cmTelescope = new CloudMonitoringTelescope(withMz);

        writer.insertFull(cmTelescope.toMetrics());
        // write them.
        writer.insertFull(cmTelescope.toMetrics());
        
        // read them back.
        int expectedWithMz = 1;
        int actualWithMz = 0;
        for (Column col : reader.getNumericRollups(locator, Granularity.FULL, baseMillis, baseMillis + 10000)) {
            actualWithMz += 1;
        }
        assertEquals(expectedWithMz, actualWithMz);
    }
    
    public void testSimpleInsertAndGet() throws Exception {
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        AstyanaxReader reader = AstyanaxReader.getInstance();
        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + CqlTestBase.randString(8);
        final String entityId = "en" + CqlTestBase.randString(8);
        final String checkName = "test_simple_insert";
        // fyi, metric names are "intmetric" and "doublemetric"
        final Locator locator  = ServerMetricLocator.createFromTelescopePrimitives(acctId, entityId, checkName,
                "dim0.intmetric");
        
        Set<Long> expectedTimestamps = new HashSet<Long>();
        // insert something every 30s for 5 mins.
        for (int i = 0; i < 10; i++) {
            final long curMillis = baseMillis + (i * 30000); // 30 seconds later.
            expectedTimestamps.add(curMillis);

            final CloudMonitoringTelescope cmTelescope = new CloudMonitoringTelescope(
                    makeTelescope("uuid", checkName, acctId, "module", entityId, "target", curMillis, "dim0"));

            writer.insertFull(cmTelescope.toMetrics());
        }
        
        Set<Long> actualTimestamps = new HashSet<Long>();
        // get back the cols that were written from start to stop.

        for (Column<Long> col : reader.getNumericRollups(locator, Granularity.FULL, baseMillis, lastMillis))
            actualTimestamps.add(col.getName());
        assertEquals(expectedTimestamps, actualTimestamps);
    }

    public void testConsecutiveWriteAndRead() throws ConnectionException, IOException {
        System.err.println("testConsecutiveWriteAndRead");
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        AstyanaxReader reader = AstyanaxReader.getInstance();
        final long baseMillis = 1333635148000L;
        Collection<Telescope> telescopes = new ArrayList<Telescope>();
        final Locator locator = ServerMetricLocator.createFromTelescopePrimitives("ac0001", "en0001", "ch0001",
                "dim0.test-metric");
        for (int i = 0; i < 100; i++) {
            Telescope t = new Telescope("tId", "ch0001", "ac0001", "http", "en0001", "www.example.com", baseMillis + (i * 1000), 1, VerificationModel.ONE);
            Map<String, Metric> metrics = new HashMap<String, Metric>();
            Metric m = new Metric((byte)'i');
            m.setValueI32(i);
            metrics.put("dim0.test_metric", m);
            t.setMetrics(metrics);
            telescopes.add(t);
            writer.insertFull(new CloudMonitoringTelescope(t).toMetrics());
            telescopes.clear();
        }
        
        int count = 0;
        for (Column<Long> col : reader.getNumericRollups(locator, Granularity.FULL, baseMillis, baseMillis + 500000)) {
            int v = (Integer)col.getValue(NumericSerializer.get(Granularity.FULL));
            assertEquals(count, v);
            count++;
        }
    }
    
    public void testShardStateWriteRead() throws Exception {
        System.err.println("testShardStateWriteRead");
        final Collection<Integer> shards = Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        AstyanaxWriter writer = AstyanaxWriter.getInstance();

        // Simulate active or running state for all the slots for all granularities.
        for (int shard : shards) {
            Map<Granularity, Map<Integer, UpdateStamp>> allUpdates = new HashMap<Granularity, Map<Integer, UpdateStamp>>();
            for (Granularity granularity : Granularity.values()) {
                if (granularity == Granularity.FULL) continue;
                Map<Integer, UpdateStamp> updates = new HashMap<Integer, UpdateStamp>();
                for (int slot = 0; slot < granularity.numSlots(); slot++) {
                    updates.put(slot, new UpdateStamp(System.currentTimeMillis() - 10000, UpdateStamp.State.Active,
                            true));
                }
                allUpdates.put(granularity, updates);
            }
            writer.persistShardState(shard, allUpdates);
        }

        // Now simulate rolled up state for all the slots for all granularities.
        for (int shard : shards) {
            Map<Granularity, Map<Integer, UpdateStamp>> allUpdates = new HashMap<Granularity, Map<Integer, UpdateStamp>>();
            for (Granularity granularity : Granularity.values()) {
                if (granularity == Granularity.FULL) continue;
                Map<Integer, UpdateStamp> updates = new HashMap<Integer, UpdateStamp>();
                for (int slot = 0; slot < granularity.numSlots(); slot++) {
                    updates.put(slot, new UpdateStamp(System.currentTimeMillis(), UpdateStamp.State.Rolled,
                            true));
                }
                allUpdates.put(granularity, updates);
            }
            writer.persistShardState(shard, allUpdates);
        }

        // Now we would have the longest row for each shard because we filled all the slots.
        // Now test whether getAllShardStates returns all the slots [https://issues.rax.io/browse/CMD-11]
        AstyanaxReader reader = AstyanaxReader.getInstance();
        int count = 0;
        for (Map.Entry<Integer, Map<Granularity, Map<Integer, UpdateStamp>>> shardStates : reader.getAllShardStates(shards).entrySet()) {
            assertEquals(Granularity.MIN_5.numSlots(), shardStates.getValue().get(Granularity.MIN_5).size());
            assertEquals(Granularity.MIN_20.numSlots(), shardStates.getValue().get(Granularity.MIN_20).size());
            assertEquals(Granularity.MIN_60.numSlots(), shardStates.getValue().get(Granularity.MIN_60).size());
            assertEquals(Granularity.MIN_240.numSlots(), shardStates.getValue().get(Granularity.MIN_240).size());
            assertEquals(Granularity.MIN_1440.numSlots(), shardStates.getValue().get(Granularity.MIN_1440).size());
            count += 1;
        }
        assertEquals(shards.size(), count);
    }
    
    public void testUpdateStampCoaelescing() throws Exception {
        final int shard = 24;
        final int slot = 16;
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        Map<Granularity, Map<Integer, UpdateStamp>> updates = new HashMap<Granularity, Map<Integer, UpdateStamp>>();
        Map<Integer, UpdateStamp> slotUpdates = new HashMap<Integer, UpdateStamp>();
        updates.put(Granularity.MIN_5, slotUpdates);
        
        long time = 1234;
        slotUpdates.put(slot, new UpdateStamp(time++, UpdateStamp.State.Active, true));
        writer.persistShardState(shard, updates);
        
        slotUpdates.put(slot, new UpdateStamp(time++, UpdateStamp.State.Rolled, true));
        writer.persistShardState(shard, updates);
        
        AstyanaxReader reader = AstyanaxReader.getInstance();
        Map<Granularity, Map<Integer, UpdateStamp>> dbShardState = reader.getAllShardStates(Lists.newArrayList(shard)).get(shard);
        assertNotNull(dbShardState.get(Granularity.MIN_5));
        assertEquals(UpdateStamp.State.Rolled, dbShardState.get(Granularity.MIN_5).get(slot).getState());
    }
}
