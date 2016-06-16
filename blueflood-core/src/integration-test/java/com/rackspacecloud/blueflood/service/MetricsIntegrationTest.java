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

import com.google.common.collect.Lists;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.rackspacecloud.blueflood.utils.Util;
import org.junit.Assert;
import org.junit.Test;

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

        LocatorIO locatorIO = IOContainer.fromConfig().getLocatorIO();

        for (Locator locator : locators) {
            locatorIO.insertLocator(locator);
        }
        return locators;
    }

    private void writeFullData(
            Locator locator,
            long baseMillis, 
            int hours,
            AbstractMetricsRW metricsRW) throws Exception {
        // insert something every minute for 48h
        for (int i = 0; i < 60 * hours; i++) {
            final long curMillis = baseMillis + i * 60000;
            List<IMetric> metrics = new ArrayList<IMetric>();
            metrics.add(getRandomIntMetric(locator, curMillis));
            metricsRW.insertMetrics(metrics);
        }
    }

    @Test
    public void testLocatorsWritten() throws Exception {
        Collection<Locator> locators = writeLocatorsOnly(48);
        LocatorIO locatorIO = IOContainer.fromConfig().getLocatorIO();

        Set<String> actualLocators = new HashSet<String>();
        for (Locator locator : locators) {
            for (Locator databaseLocator : locatorIO.getLocators(Util.computeShard(locator.toString()))) {
                actualLocators.add(databaseLocator.toString());
            }
        }
        Assert.assertEquals(48, actualLocators.size());
    }

    @Test
    public void testRollupGenerationSimple() throws Exception {
        AbstractMetricsRW basicMetricsRW = IOContainer.fromConfig().getBasicMetricsRW();

        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        int hours = 48;
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);
        final long endMillis = baseMillis + (1000 * 60 * 60 * hours);
        final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);

        writeFullData(locator, baseMillis, hours, basicMetricsRW);

        // FULL -> 5m
        ArrayList<SingleRollupWriteContext> writes = new ArrayList<SingleRollupWriteContext>();
        for (Range range : Range.getRangesToRollup(Granularity.FULL, baseMillis, endMillis)) {
            // each range should produce one average
            Points<SimpleNumber> input = basicMetricsRW.getDataToRollup(locator, RollupType.BF_BASIC,
                    range, CassandraModel.CF_METRICS_FULL_NAME);
            BasicRollup basicRollup = BasicRollup.buildRollupFromRawSamples(input);

            writes.add(new SingleRollupWriteContext(basicRollup,
                    locator,
                    Granularity.FULL.coarser(),
                    CassandraModel.getColumnFamily(BasicRollup.class, Granularity.FULL.coarser()),
                    range.start));
        }
        basicMetricsRW.insertRollups(writes);

        // 5m -> 20m
        writes.clear();

        for (Range range : Range.getRangesToRollup(Granularity.MIN_5, baseMillis, endMillis)) {
            Points<BasicRollup> input = basicMetricsRW.getDataToRollup(locator, RollupType.BF_BASIC, range,
                    CassandraModel.getBasicColumnFamilyName(Granularity.MIN_5));
            BasicRollup basicRollup = BasicRollup.buildRollupFromRollups(input);
            writes.add(new SingleRollupWriteContext(basicRollup,
                    locator,
                    Granularity.MIN_5.coarser(),
                    CassandraModel.getColumnFamily(BasicRollup.class, Granularity.MIN_5.coarser()),
                    range.start));
        }
        basicMetricsRW.insertRollups(writes);

        // 20m -> 60m
        writes.clear();
        for (Range range : Range.getRangesToRollup(Granularity.MIN_20, baseMillis, endMillis)) {
            Points<BasicRollup> input = basicMetricsRW.getDataToRollup(locator, RollupType.BF_BASIC, range,
                    CassandraModel.getBasicColumnFamilyName(Granularity.MIN_20));
            BasicRollup basicRollup = BasicRollup.buildRollupFromRollups(input);
            writes.add(new SingleRollupWriteContext(basicRollup,
                    locator,
                    Granularity.MIN_20.coarser(),
                    CassandraModel.getColumnFamily(BasicRollup.class, Granularity.MIN_20.coarser()),
                    range.start));
        }
        basicMetricsRW.insertRollups(writes);

        // 60m -> 240m
        writes.clear();
        for (Range range : Range.getRangesToRollup(Granularity.MIN_60, baseMillis, endMillis)) {
            Points<BasicRollup> input = basicMetricsRW.getDataToRollup(locator, RollupType.BF_BASIC, range,
                    CassandraModel.getBasicColumnFamilyName(Granularity.MIN_60));

            BasicRollup basicRollup = BasicRollup.buildRollupFromRollups(input);
            writes.add(new SingleRollupWriteContext(basicRollup,
                    locator,
                    Granularity.MIN_60.coarser(),
                    CassandraModel.getColumnFamily(BasicRollup.class, Granularity.MIN_60.coarser()),
                    range.start));
        }
        basicMetricsRW.insertRollups(writes);

        // 240m -> 1440m
        writes.clear();
        for (Range range : Range.getRangesToRollup(Granularity.MIN_240, baseMillis, endMillis)) {
            Points<BasicRollup> input = basicMetricsRW.getDataToRollup(locator, RollupType.BF_BASIC, range,
                    CassandraModel.getBasicColumnFamilyName(Granularity.MIN_240));
            BasicRollup basicRollup = BasicRollup.buildRollupFromRollups(input);
            writes.add(new SingleRollupWriteContext(basicRollup,
                    locator,
                    Granularity.MIN_240.coarser(),
                    CassandraModel.getColumnFamily(BasicRollup.class, Granularity.MIN_240.coarser()),
                    range.start));
        }
        basicMetricsRW.insertRollups(writes);

        // verify the number of points in 48h worth of rollups. 
        Range range = new Range(Granularity.MIN_1440.snapMillis(baseMillis), Granularity.MIN_1440.snapMillis(endMillis + Granularity.MIN_1440.milliseconds()));
        Points<BasicRollup> input = basicMetricsRW.getDataToRollup(locator, RollupType.BF_BASIC, range,
                CassandraModel.getBasicColumnFamilyName(Granularity.MIN_1440));
        BasicRollup basicRollup = BasicRollup.buildRollupFromRollups(input);
        Assert.assertEquals(60 * hours, basicRollup.getCount());
    }

    @Test
    public void testSimpleInsertAndGet() throws Exception {
        AbstractMetricsRW metricsRW = IOContainer.fromConfig().getBasicMetricsRW();

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
            List<IMetric> metrics = new ArrayList<IMetric>();
            metrics.add(getRandomIntMetric(locator, curMillis));
            metricsRW.insertMetrics(metrics);
        }

        // get back the cols that were written from start to stop.

        Points<SimpleNumber> points = metricsRW.getDataToRollup(locator, RollupType.BF_BASIC, new Range(baseMillis, lastMillis),
                CassandraModel.getBasicColumnFamilyName(Granularity.FULL));
        Set<Long> actualTimestamps = points.getPoints().keySet();
        Assert.assertEquals(expectedTimestamps, actualTimestamps);
    }


    @Test
    public void testConsecutiveWriteAndRead() throws Exception {

        final long baseMillis = 1333635148000L;

        final Locator locator = Locator.createLocatorFromPathComponents("ac0001",
                "fooService,fooServer," + randString(8));

        AbstractMetricsRW metricsRW = IOContainer.fromConfig().getBasicMetricsRW();

        final List<IMetric> metrics = new ArrayList<IMetric>();
        for (int i = 0; i < 100; i++) {
            final Metric metric = new Metric(locator, i, baseMillis + (i * 1000),
                    new TimeValue(1, TimeUnit.DAYS), "unknown");
            metrics.add(metric);
            metricsRW.insertMetrics(metrics);
            metrics.clear();
        }

        int count = 0;
        Points<SimpleNumber> points = metricsRW.getDataToRollup(locator, RollupType.BF_BASIC,
                new Range(baseMillis, baseMillis + 500000), CassandraModel.CF_METRICS_FULL_NAME);
        for (Map.Entry<Long, Points.Point<SimpleNumber>> data : points.getPoints().entrySet()) {
            Points.Point<SimpleNumber> point = data.getValue();
            Assert.assertEquals(count, point.getData().getValue());
            count++;
        }
    }

    @Test
    public void testShardStateWriteRead() throws Exception {
        final Collection<Integer> shards = Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        ShardStateIO shardStateIO = IOContainer.fromConfig().getShardStateIO();

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
            shardStateIO.putShardState(shard, allUpdates);
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
            shardStateIO.putShardState(shard, allUpdates);
        }

        // Now we would have the longest row for each shard because we filled all the slots.
        // Now test whether getShardState returns all the slots
        ScheduleContext ctx = new ScheduleContext(System.currentTimeMillis(), shards);
        ShardStateManager shardStateManager = ctx.getShardStateManager();

        for (Integer shard : shards) {
            Collection<SlotState> slotStates = shardStateIO.getShardState(shard);
            for (SlotState slotState : slotStates) {
                shardStateManager.updateSlotOnRead(shard, slotState);
            }

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
        ShardStateIO shardStateIO = IOContainer.fromConfig().getShardStateIO();
        Map<Granularity, Map<Integer, UpdateStamp>> updates = new HashMap<Granularity, Map<Integer, UpdateStamp>>();
        Map<Integer, UpdateStamp> slotUpdates = new HashMap<Integer, UpdateStamp>();
        updates.put(Granularity.MIN_5, slotUpdates);
        
        long time = 1234;
        slotUpdates.put(slot, new UpdateStamp(time++, UpdateStamp.State.Active, true));
        shardStateIO.putShardState(shard, updates);
        
        slotUpdates.put(slot, new UpdateStamp(time++, UpdateStamp.State.Rolled, true));
        shardStateIO.putShardState(shard, updates);

        ScheduleContext ctx = new ScheduleContext(System.currentTimeMillis(), Lists.newArrayList(shard));

        Collection<SlotState> slotStates = shardStateIO.getShardState(shard);
        for (SlotState slotState : slotStates) {
            ctx.getShardStateManager().updateSlotOnRead(shard, slotState);
        }

        ShardStateManager shardStateManager = ctx.getShardStateManager();
        ShardStateManager.SlotStateManager slotStateManager = shardStateManager.getSlotStateManager(shard, Granularity.MIN_5);

        Assert.assertNotNull(slotStateManager.getSlotStamps());
        Assert.assertEquals(UpdateStamp.State.Active, slotStateManager.getSlotStamps().get(slot).getState());
    }
}
