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

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.rackspacecloud.blueflood.utils.Util;
import com.google.common.collect.Lists;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Some of these tests here were horribly contrived to mimic behavior in Writer. The problem with this approach is that
 * when logic in Writer changes, these tests can break unless the logic is changed here too. */
public class MetricsIntegrationTest extends IntegrationTestBase {
    
    // returns a collection all checks that were written at some point.
    // this was a lot cooler back when the slot changed with time.
    private Collection<Locator> writeLocatorsOnly(int hours) throws Exception {
        // duplicate the logic from Writer.writeFull() that inserts locator rows.
        final String tenantId = "ac" + randString(8);
        final List<Locator> locators = new ArrayList<Locator>();
        for (int i = 0; i < hours; i++) {
            locators.add(Locator.createLocatorFromPathComponents(tenantId, "test:locator:inserts:" + i));
        }

        AstyanaxTester at = new AstyanaxTester();
        MutationBatch mb = at.createMutationBatch();

        for (Locator locator : locators) {
            int shard = Util.computeShard(locator.toString());
            mb.withRow(at.getLocatorCF(), (long)shard)
                    .putColumn(locator, "", 100000);
        }
        mb.execute();

        return locators;
    }

    private void writeFullData(
            Locator locator,
            long baseMillis, 
            int hours,
            AstyanaxWriter writer) throws Exception {
        // insert something every minute for 48h
        for (int i = 0; i < 60 * hours; i++) {
            final long curMillis = baseMillis + i * 60000;
            List<Metric> metrics = new ArrayList<Metric>();
            metrics.add(getRandomIntMetric(locator, curMillis));
            writer.insertFull(metrics);
        }
    }

    @Test
    public void testLocatorsWritten() throws Exception {
        Collection<Locator> locators = writeLocatorsOnly(48);
        AstyanaxReader r = AstyanaxReader.getInstance();

        Set<String> actualLocators = new HashSet<String>();
        for (Locator locator : locators) {
            for (Column<Locator> locatorCol : r.getAllLocators(Util.computeShard(locator.toString()))) {
                actualLocators.add(locatorCol.getName().toString());
            }
        }
        Assert.assertEquals(48, actualLocators.size());
    }

    @Test
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
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);
        final long endMillis = baseMillis + (60 * 60 * hours * 1000);
        final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);

        writeFullData(locator, baseMillis, hours,  writer);
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
            ColumnFamily<Locator, Long> CF = AstyanaxIO.getColumnFamilyMapper().get(gran.name());
            // todo: use serializer specific method when available.
            AbstractSerializer serializer = CF.equals(AstyanaxIO.CF_METRICS_FULL) 
                    ? NumericSerializer.serializerFor(SimpleNumber.class) 
                    : NumericSerializer.serializerFor(BasicRollup.class);
            for (Column<Long> col : reader.getNumericRollups(locator,
                    CF,
                    macroRange.start,
                    macroRange.stop)) {
                cols.put(col.getName(), col.getValue(serializer));
            }
            BasicRollup basicRollup = new BasicRollup();
            Map<Long, BasicRollup> rollups = new HashMap<Long, BasicRollup>();
            for (Map.Entry<Long, Object> col : cols.entrySet()) {
                while (col.getKey() > curRange.stop) {
                    rollups.put(curRange.start, basicRollup);
                    basicRollup = new BasicRollup();
                    curRange = ranges.remove(0);
                }

                Points input;
                if (gran == Granularity.FULL) {
                    input = new Points<SimpleNumber>();
                    Object longOrDouble = col.getValue();
                    input.add(new Points.Point<SimpleNumber>(col.getKey(), new SimpleNumber(col.getValue())));
                    basicRollup.computeFromSimpleMetricsUnsafe(input);
                } else {
                    input = new Points<BasicRollup>();
                    BasicRollup desBasicRollup = (BasicRollup)col.getValue();
                    input.add(new Points.Point<BasicRollup>(col.getKey(), desBasicRollup));
                    basicRollup.computeFromRollupsUnsafe(input);
                }
            }

            rollups.put(curRange.start, basicRollup);
            writer.insertRollups(locator, rollups, AstyanaxIO.getColumnFamilyMapper().get(gran.coarser().name()));
        }
        
        // verify the number of points in 48h worth of rollups. 
        Range range = new Range(Granularity.MIN_1440.snapMillis(baseMillis), Granularity.MIN_1440.snapMillis(endMillis + Granularity.MIN_1440.milliseconds()));
        Points<BasicRollup> input = reader.getBasicRollupDataToRoll(locator, range, AstyanaxIO.getColumnFamilyMapper().get(Granularity.MIN_1440.name()));
        BasicRollup basicRollup = BasicRollup.buildRollupFromRollups(input);
        Assert.assertEquals(60 * hours, basicRollup.getCount());
    }

    @Test
    public void testRollupGenerationSimple() throws Exception {
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        AstyanaxReader reader = AstyanaxReader.getInstance();
        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        int hours = 48;
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);
        final long endMillis = baseMillis + (1000 * 60 * 60 * hours);
        final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);

        writeFullData(locator, baseMillis, hours, writer);

        // FULL -> 5m
        Map<Long, BasicRollup> rollups = new HashMap<Long, BasicRollup>();
        for (Range range : Range.getRangesToRollup(Granularity.FULL, baseMillis, endMillis)) {
            // each range should produce one average
            Points<SimpleNumber> input = reader.getSimpleDataToRoll(locator, range);
            BasicRollup basicRollup = BasicRollup.buildRollupFromRawSamples(input);
            rollups.put(range.start, basicRollup);
        }
        writer.insertRollups(locator, rollups,
                AstyanaxIO.getColumnFamilyMapper().get(Granularity.FULL.coarser().name()));

        // 5m -> 20m
        rollups.clear();
        for (Range range : Range.getRangesToRollup(Granularity.MIN_5, baseMillis, endMillis)) {
            Points<BasicRollup> input = reader.getBasicRollupDataToRoll(locator, range,
                    AstyanaxIO.getColumnFamilyMapper().get(Granularity.MIN_5.name()));
            BasicRollup basicRollup = BasicRollup.buildRollupFromRollups(input);
            rollups.put(range.start, basicRollup);
        }

        writer.insertRollups(locator, rollups, AstyanaxIO.getColumnFamilyMapper().get(Granularity.MIN_5.coarser().name()));

        // 20m -> 60m
        rollups.clear();
        for (Range range : Range.getRangesToRollup(Granularity.MIN_20, baseMillis, endMillis)) {
            Points<BasicRollup> input = reader.getBasicRollupDataToRoll(locator, range,
                    AstyanaxIO.getColumnFamilyMapper().get(Granularity.MIN_20.name()));
            BasicRollup basicRollup = BasicRollup.buildRollupFromRollups(input);
            rollups.put(range.start, basicRollup);
        }
        writer.insertRollups(locator, rollups,
                AstyanaxIO.getColumnFamilyMapper().get(Granularity.MIN_20.coarser().name()));

        // 60m -> 240m
        rollups.clear();
        for (Range range : Range.getRangesToRollup(Granularity.MIN_60, baseMillis, endMillis)) {
            Points<BasicRollup> input = reader.getBasicRollupDataToRoll(locator, range,
                    AstyanaxIO.getColumnFamilyMapper().get(Granularity.MIN_60.name()));

            BasicRollup basicRollup = BasicRollup.buildRollupFromRollups(input);
            rollups.put(range.start, basicRollup);
        }
        writer.insertRollups(locator, rollups,
                AstyanaxIO.getColumnFamilyMapper().get(Granularity.MIN_60.coarser().name()));

        // 240m -> 1440m
        rollups.clear();
        for (Range range : Range.getRangesToRollup(Granularity.MIN_240, baseMillis, endMillis)) {
            Points<BasicRollup> input = reader.getBasicRollupDataToRoll(locator, range,
                    AstyanaxIO.getColumnFamilyMapper().get(Granularity.MIN_240.name()));
            BasicRollup basicRollup = BasicRollup.buildRollupFromRollups(input);
            rollups.put(range.start, basicRollup);
        }
        writer.insertRollups(locator, rollups,
                AstyanaxIO.getColumnFamilyMapper().get(Granularity.MIN_240.coarser().name()));

        // verify the number of points in 48h worth of rollups. 
        Range range = new Range(Granularity.MIN_1440.snapMillis(baseMillis), Granularity.MIN_1440.snapMillis(endMillis + Granularity.MIN_1440.milliseconds()));
        Points<BasicRollup> input = reader.getBasicRollupDataToRoll(locator, range,
                AstyanaxIO.getColumnFamilyMapper().get(Granularity.MIN_1440.name()));
        BasicRollup basicRollup = BasicRollup.buildRollupFromRollups(input);
        Assert.assertEquals(60 * hours, basicRollup.getCount());
    }

    @Test
    public void testSimpleInsertAndGet() throws Exception {
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        AstyanaxReader reader = AstyanaxReader.getInstance();
        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);

        final Locator locator  = Locator.createLocatorFromPathComponents(acctId, metricName);

        Set<Long> expectedTimestamps = new HashSet<Long>();
        // insert something every 30s for 5 mins.
        for (int i = 0; i < 10; i++) {
            final long curMillis = baseMillis + (i * 30000); // 30 seconds later.
            expectedTimestamps.add(curMillis);
            List<Metric> metrics = new ArrayList<Metric>();
            metrics.add(getRandomIntMetric(locator, curMillis));
            writer.insertFull(metrics);
        }
        
        Set<Long> actualTimestamps = new HashSet<Long>();
        // get back the cols that were written from start to stop.

        for (Column<Long> col : reader.getNumericRollups(locator,
                AstyanaxIO.getColumnFamilyMapper().get(Granularity.FULL.name()), baseMillis, lastMillis))
            actualTimestamps.add(col.getName());
        Assert.assertEquals(expectedTimestamps, actualTimestamps);
    }

    @Test
    public void testConsecutiveWriteAndRead() throws ConnectionException, IOException {
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        AstyanaxReader reader = AstyanaxReader.getInstance();
        final long baseMillis = 1333635148000L;

        final Locator locator = Locator.createLocatorFromPathComponents("ac0001",
                "fooService,fooServer," + randString(8));

        final List<Metric> metrics = new ArrayList<Metric>();
        for (int i = 0; i < 100; i++) {
            final Metric metric = new Metric(locator, i, baseMillis + (i * 1000),
                    new TimeValue(1, TimeUnit.DAYS), "unknown");
            metrics.add(metric);
            writer.insertFull(metrics);
            metrics.clear();
        }
        
        int count = 0;
        ColumnFamily<Locator, Long> CF_metrics_full = AstyanaxIO.getColumnFamilyMapper().get(Granularity.FULL.name());
        for (Column<Long> col : reader.getNumericRollups(locator, CF_metrics_full, baseMillis, baseMillis + 500000)) {
            int v = col.getValue(NumericSerializer.serializerFor(Integer.class));
            Assert.assertEquals(count, v);
            count++;
        }
    }

    @Test
    public void testShardStateWriteRead() throws Exception {
        final Collection<Integer> shards = Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        AstyanaxWriter writer = AstyanaxWriter.getInstance();

        // Simulate active or running state for all the slots for all granularities.
        for (int shard : shards) {
            Map<Granularity, Map<Integer, UpdateStamp>> allUpdates = new HashMap<Granularity, Map<Integer, UpdateStamp>>();
            for (Granularity granularity : Granularity.rollupGranularities()) {
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
            for (Granularity granularity : Granularity.rollupGranularities()) {
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
        // Now test whether getAndUpdateAllShardStates returns all the slots [https://issues.rax.io/browse/CMD-11]
        AstyanaxReader reader = AstyanaxReader.getInstance();
        ScheduleContext ctx = new ScheduleContext(System.currentTimeMillis(), shards);
        ShardStateManager shardStateManager = ctx.getShardStateManager();

        reader.getAndUpdateAllShardStates(ctx.getShardStateManager(), shards);

        for (Integer shard : shards) {
            for (Granularity granularity : Granularity.rollupGranularities()) {
                ShardStateManager.SlotStateManager slotStateManager = shardStateManager.getSlotStateManager(shard, granularity);
                Assert.assertEquals(granularity.numSlots(), slotStateManager.getSlotStamps().size());
            }
        }
    }

    @Test
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
        //Map<Granularity, Map<Integer, UpdateStamp>> dbShardState = reader.getAndUpdateAllShardStates(Lists.newArrayList(shard)).get(shard);
        ScheduleContext ctx = new ScheduleContext(System.currentTimeMillis(), Lists.newArrayList(shard));
        reader.getAndUpdateAllShardStates(ctx.getShardStateManager(), Lists.newArrayList(shard));
        ShardStateManager shardStateManager = ctx.getShardStateManager();
        ShardStateManager.SlotStateManager slotStateManager = shardStateManager.getSlotStateManager(shard, Granularity.MIN_5);

        Assert.assertNotNull(slotStateManager.getSlotStamps());
        Assert.assertEquals(UpdateStamp.State.Rolled, slotStateManager.getSlotStamps().get(slot).getState());
    }
}
