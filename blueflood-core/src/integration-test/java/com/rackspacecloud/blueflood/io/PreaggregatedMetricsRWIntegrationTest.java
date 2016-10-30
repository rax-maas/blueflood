/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspacecloud.blueflood.io;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.io.astyanax.APreaggregatedMetricsRW;
import com.rackspacecloud.blueflood.io.datastax.DDelayedLocatorIO;
import com.rackspacecloud.blueflood.io.datastax.DEnumIO;
import com.rackspacecloud.blueflood.io.datastax.DLocatorIO;
import com.rackspacecloud.blueflood.io.datastax.DPreaggregatedMetricsRW;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.rackspacecloud.blueflood.utils.Util;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A class to test the various implementation of PreaggregatedMetricsRW.
 */
public class PreaggregatedMetricsRWIntegrationTest extends IntegrationTestBase {

    private static final String TENANT1 = "123456";
    private static final String TENANT2 = "987654";
    private static final String TENANT3 = "123789";
    private static final TimeValue TTL = new TimeValue(24, TimeUnit.HOURS);

    protected LocatorIO locatorIO = new DLocatorIO();
    protected DelayedLocatorIO delayedLocatorIO = new DDelayedLocatorIO();
    protected DEnumIO enumIO = new DEnumIO();
    protected DPreaggregatedMetricsRW datastaxMetricsRW = new DPreaggregatedMetricsRW(enumIO, locatorIO, delayedLocatorIO, false, new DefaultClockImpl());
    protected APreaggregatedMetricsRW astyanaxMetricsRW = new APreaggregatedMetricsRW(false, new DefaultClockImpl());

    protected static final long MAX_AGE_ALLOWED = Configuration.getInstance().getLongProperty(CoreConfig.ROLLUP_DELAY_MILLIS);

    protected static Granularity DELAYED_METRICS_STORAGE_GRANULARITY =
            Granularity.getRollupGranularity(Configuration.getInstance().getStringProperty(CoreConfig.DELAYED_METRICS_STORAGE_GRANULARITY));


    protected Map<Locator, IMetric>  expectedLocatorMetricMap = new HashMap<Locator, IMetric>();
    protected IMetric timerMetric;
    protected long startTimestamp;

    @Before
    public void generateMetrics() throws Exception {
        startTimestamp = System.currentTimeMillis();

        Locator locator;
        String  className = this.getClass().getSimpleName();
        for ( String tenantId : Arrays.asList(TENANT1, TENANT2, TENANT3) ) {

            // generate counter
            BluefloodCounterRollup counterRollup = new BluefloodCounterRollup()
                    .withCount(RAND.nextInt(10))
                    .withRate(RAND.nextDouble())
                    .withSampleCount(RAND.nextInt(10));
            locator = Locator.createLocatorFromPathComponents(tenantId, className + ".my.metric.counter." + System.currentTimeMillis());
            PreaggregatedMetric pMetric = new PreaggregatedMetric(System.currentTimeMillis(), locator, new TimeValue(1, TimeUnit.DAYS), counterRollup);
            expectedLocatorMetricMap.put(locator, pMetric);
            MetadataCache.getInstance().put(locator, MetricMetadata.TYPE.name().toLowerCase(), null);
            MetadataCache.getInstance().put(locator, MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.COUNTER.toString());

            // generate enum
            pMetric = getEnumMetric(this.getClass().getSimpleName() + ".my.enum.values." + System.currentTimeMillis(), tenantId, startTimestamp);
            expectedLocatorMetricMap.put(pMetric.getLocator(), pMetric);
            MetadataCache.getInstance().put(pMetric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), null);
            MetadataCache.getInstance().put(pMetric.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.ENUM.toString());

            // generate gauge
            BluefloodGaugeRollup gaugeRollup = new BluefloodGaugeRollup()
                    .withLatest(System.currentTimeMillis(), RAND.nextLong());
            gaugeRollup.setVariance(RAND.nextDouble());
            locator = Locator.createLocatorFromPathComponents(tenantId, className + ".my.metric.gauge." + System.currentTimeMillis());
            pMetric = new PreaggregatedMetric(System.currentTimeMillis(), locator, new TimeValue(1, TimeUnit.DAYS), gaugeRollup);
            expectedLocatorMetricMap.put(locator, pMetric);
            MetadataCache.getInstance().put(locator, MetricMetadata.TYPE.name().toLowerCase(), null);
            MetadataCache.getInstance().put(locator, MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.GAUGE.toString());

            // generate set
            Set aSet = Sets.newHashSet(RAND.nextInt(), RAND.nextInt(), RAND.nextInt());
            BluefloodSetRollup setRollup = new BluefloodSetRollup().withObject(aSet);
            locator = Locator.createLocatorFromPathComponents(tenantId, className + ".my.metric.set." + System.currentTimeMillis());
            pMetric = new PreaggregatedMetric(System.currentTimeMillis(), locator, new TimeValue(1, TimeUnit.DAYS), setRollup);
            expectedLocatorMetricMap.put(locator, pMetric);
            MetadataCache.getInstance().put(locator, MetricMetadata.TYPE.name().toLowerCase(), null);
            MetadataCache.getInstance().put(locator, MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.SET.toString());

            // generate timer
            BluefloodTimerRollup timerRollup = new BluefloodTimerRollup()
                    .withSampleCount(RAND.nextInt(10))
                    .withSum(RAND.nextDouble())
                    .withCountPS(RAND.nextDouble())
                    .withAverage(RAND.nextLong())
                    .withVariance(RAND.nextDouble())
                    .withMinValue(RAND.nextInt())
                    .withMaxValue(RAND.nextInt())
                    .withCount(RAND.nextInt(200));
            locator = Locator.createLocatorFromPathComponents(tenantId, className + ".my.metric.timer." + System.currentTimeMillis());
            timerMetric = new PreaggregatedMetric(System.currentTimeMillis(), locator, new TimeValue(1, TimeUnit.DAYS), timerRollup);
            expectedLocatorMetricMap.put(locator, timerMetric);
            MetadataCache.getInstance().put(locator, MetricMetadata.TYPE.name().toLowerCase(), null);
            MetadataCache.getInstance().put(locator, MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.TIMER.toString());
        }
    }

    /**
     * This method is to supply the granularity parameter to some test methods below
     *
     * @return
     */
    protected Object getGranularitiesToTest() {
        return Granularity.granularities();
    }

    /**
     * Converts the input metrics from a map of locator -> IMetric to a list of
     * {@link com.rackspacecloud.blueflood.service.SingleRollupWriteContext}
     * objects
     *
     * @param inputMetrics
     * @return
     */
    protected List<SingleRollupWriteContext> toWriteContext(Map<Locator, IMetric> inputMetrics, Granularity destGran) {

        List<SingleRollupWriteContext> resultList = new ArrayList<SingleRollupWriteContext>();
        for ( Map.Entry<Locator, IMetric> entry : inputMetrics.entrySet() ) {
            Locator locator = entry.getKey();
            IMetric metric = entry.getValue();

            SingleRollupWriteContext writeContext =
                    new SingleRollupWriteContext(
                            (Rollup) metric.getMetricValue(),
                            locator,
                            destGran,
                            CassandraModel.getPreaggregatedColumnFamily(destGran),
                            metric.getCollectionTime());
            resultList.add(writeContext);
        }
        return resultList;

    }

    protected Collection<IMetric> toIMetricsCollection(Locator locator, Points<BluefloodTimerRollup> points) {
        List<IMetric> list = new ArrayList<IMetric>();
        for (Map.Entry<Long, Points.Point<BluefloodTimerRollup>> entry : points.getPoints().entrySet()) {
            PreaggregatedMetric metric = new PreaggregatedMetric(entry.getKey(), locator, TTL, entry.getValue().getData());
            list.add(metric);
        }
        return list;
    }

    /**
     * For a given list of locators, figure the shard they belong to and for all those shards
     * get all the locators in metric_locator column family
     *
     * @param ingestedLocators
     * @return
     * @throws IOException
     */
    protected Set<Locator> retrieveLocators(Set<Locator> ingestedLocators) throws IOException {

        Set<Long> shards = new HashSet<Long>();
        for (Locator locator: ingestedLocators) {
            long shard = (long) Util.getShard(locator.toString());
            shards.add(shard);
        }

        Set<Locator> locatorsFromDB = new HashSet<Locator>();
        for (Long shard: shards) {
            locatorsFromDB.addAll(locatorIO.getLocators(shard));
        }

        return locatorsFromDB;
    }

    /**
     * For a given list of metrics, figure out the shard and slot they belong to and for those
     * shard and slot combinations, get all the locators from metrics_delayed_locator column family.
     *
     * @param metrics
     * @return
     * @throws IOException
     */
    protected Set<Locator> retrieveLocatorsByShardAndSlot(List<IMetric> metrics) throws IOException {
        Set<String> slotKeys = new HashSet<String>();

        for (IMetric metric: metrics) {
            int shard = Util.getShard(metric.getLocator().toString());
            int slot = DELAYED_METRICS_STORAGE_GRANULARITY.slot(metric.getCollectionTime());
            SlotKey slotKey = SlotKey.of(DELAYED_METRICS_STORAGE_GRANULARITY, slot, shard);

            slotKeys.add(slotKey.toString());
        }

        Set<Locator> locatorsFromDB = new HashSet<Locator>();
        for (String slotKeyStr:  slotKeys) {
            locatorsFromDB.addAll(delayedLocatorIO.getLocators(SlotKey.parse(slotKeyStr)));
        }

        return locatorsFromDB;
    }

    /**
     * A class that test cross-driver data correctness. This one has all the data
     * written using Datastax and read using Astyanax.
     *
     */
    @RunWith(JUnitParamsRunner.class)
    public static class WriteDatastaxReadAstyanax extends PreaggregatedMetricsRWIntegrationTest {

        @Test
        @Parameters(method = "getGranularitiesToTest")
        public void testMultiMetricsDatapointsRange(Granularity granularity) throws Exception {

            // write with datastax
            datastaxMetricsRW.insertMetrics(expectedLocatorMetricMap.values(), granularity);

            // read with astyanaxRW.getDatapointsForRange()
            List<Locator> locators = new ArrayList<Locator>() {{
                addAll(expectedLocatorMetricMap.keySet());
            }};
            Map<Locator, MetricData> results = astyanaxMetricsRW.getDatapointsForRange(
                    locators,
                    getRangeFromMinAgoToNow(5),
                    granularity);

            Assert.assertEquals("number of locators", expectedLocatorMetricMap.keySet().size(), results.keySet().size());

            for ( Map.Entry<Locator, IMetric> entry : expectedLocatorMetricMap.entrySet() ) {
                Locator locator = entry.getKey();

                MetricData metricData = results.get(locator);
                Assert.assertNotNull(String.format("metric data for locator %s exists", locator), metricData);

                Points points = metricData.getData();
                Map<Long, Points.Point> pointMap = points.getPoints();
                Assert.assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

                IMetric expectedMetric = entry.getValue();
                Assert.assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                        pointMap.get(expectedMetric.getCollectionTime())));

                Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
                Assert.assertTrue(String.format("locator %s data is the same", locator), expectedMetric.getMetricValue().equals(point.getData()));
            }

            Set<Locator> ingestedLocators = expectedLocatorMetricMap.keySet();
            Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
            assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));

            Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(new ArrayList<IMetric>(expectedLocatorMetricMap.values()));
            assertTrue("Locators which are not delayed identified as delayed", Collections.disjoint(locatorsFromDBByShardAndSlot, locators));
        }

        @Test
        @Parameters(method = "getGranularitiesToTest")
        public void testMultiMetricsDatapointsRangeWithDelayedMetrics(Granularity granularity) throws Exception {

            Clock clock = mock(Clock.class);
            final Locator locator1 = expectedLocatorMetricMap.keySet().iterator().next();
            when(clock.now()).thenReturn(new Instant(expectedLocatorMetricMap.get(locator1).getCollectionTime() + MAX_AGE_ALLOWED + 1000));

            // write with datastax
            DPreaggregatedMetricsRW datastaxMetricsRW1 = new DPreaggregatedMetricsRW(enumIO, locatorIO, delayedLocatorIO, true, clock);
            datastaxMetricsRW1.insertMetrics(expectedLocatorMetricMap.values(), granularity);

            // read with astyanaxRW.getDatapointsForRange()
            List<Locator> locators = new ArrayList<Locator>() {{
                addAll(expectedLocatorMetricMap.keySet());
            }};
            Map<Locator, MetricData> results = astyanaxMetricsRW.getDatapointsForRange(
                    locators,
                    getRangeFromMinAgoToNow(5),
                    granularity);

            Assert.assertEquals("number of locators", expectedLocatorMetricMap.keySet().size(), results.keySet().size());

            for ( Map.Entry<Locator, IMetric> entry : expectedLocatorMetricMap.entrySet() ) {
                Locator locator = entry.getKey();

                MetricData metricData = results.get(locator);
                Assert.assertNotNull(String.format("metric data for locator %s exists", locator), metricData);

                Points points = metricData.getData();
                Map<Long, Points.Point> pointMap = points.getPoints();
                Assert.assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

                IMetric expectedMetric = entry.getValue();
                Assert.assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                        pointMap.get(expectedMetric.getCollectionTime())));

                Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
                Assert.assertTrue(String.format("locator %s data is the same", locator), expectedMetric.getMetricValue().equals(point.getData()));
            }

            Set<Locator> ingestedLocators = expectedLocatorMetricMap.keySet();
            Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
            assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));

            Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(new ArrayList<IMetric>(expectedLocatorMetricMap.values()));
            locatorsFromDBByShardAndSlot.retainAll(ingestedLocators);
            assertEquals("Locators which are not delayed identified as delayed", locators.size(), locatorsFromDBByShardAndSlot.size());
        }

        @Test
        @Parameters(method = "getGranularitiesToTest")
        public void testInsertRollups(Granularity granularity) throws Exception {

            // write with datastax
            List<SingleRollupWriteContext> writeContexts = toWriteContext(expectedLocatorMetricMap, granularity);
            datastaxMetricsRW.insertRollups(writeContexts);

            List<Locator> locators = new ArrayList<Locator>() {{
                addAll(expectedLocatorMetricMap.keySet());
            }};
            // read with astyanax
            Map<Locator, MetricData> results = astyanaxMetricsRW.getDatapointsForRange(
                    locators,
                    getRangeFromMinAgoToNow(5),
                    granularity);

            Assert.assertEquals("number of locators", expectedLocatorMetricMap.keySet().size(), results.keySet().size());

            for ( Map.Entry<Locator, IMetric> entry : expectedLocatorMetricMap.entrySet() ) {
                Locator locator = entry.getKey();

                MetricData metricData = results.get(locator);
                Assert.assertNotNull(String.format("metric data for locator %s exists", locator), metricData);

                Points points = metricData.getData();
                Map<Long, Points.Point> pointMap = points.getPoints();
                Assert.assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

                IMetric expectedMetric = entry.getValue();
                Assert.assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                        pointMap.get(expectedMetric.getCollectionTime())));

                Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
                Assert.assertTrue(String.format("locator %s data is the same", locator), expectedMetric.getMetricValue().equals(point.getData()));
            }
        }

        @Test
        public void testSingleMetricDatapointForRange() throws Exception {

            // write with datastax
            datastaxMetricsRW.insertMetrics(expectedLocatorMetricMap.values());

            // pick first locator from input metrics, read with Astyanax.getDataToRollup
            final Locator locator = expectedLocatorMetricMap.keySet().iterator().next();
            IMetric expectedMetric = expectedLocatorMetricMap.get(locator);
            MetricData metricData =
                    astyanaxMetricsRW.getDatapointsForRange (locator,
                            getRangeFromMinAgoToNow(5),
                            Granularity.FULL);

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            Assert.assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

            Assert.assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                    pointMap.get(expectedMetric.getCollectionTime())));

            Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
            Assert.assertTrue(String.format("locator %s data is the same", locator), expectedMetric.getMetricValue().equals(point.getData()));

            Set<Locator> ingestedLocators = new HashSet<Locator>(){{ add(locator); }};
            Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
            assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));
        }

        @Test
        public void testSingleMetricDatapointForRangeWithDelayedMetrics() throws Exception {

            Clock clock = mock(Clock.class);
            final Locator locator = expectedLocatorMetricMap.keySet().iterator().next();
            when(clock.now()).thenReturn(new Instant(expectedLocatorMetricMap.get(locator).getCollectionTime() + MAX_AGE_ALLOWED + 1));

            // write with datastax
            DPreaggregatedMetricsRW datastaxMetricsRW1 = new DPreaggregatedMetricsRW(enumIO, locatorIO, delayedLocatorIO, true, clock);
            datastaxMetricsRW1.insertMetrics(expectedLocatorMetricMap.values());

            // pick first locator from input metrics, read with Astyanax.getDataToRollup
            IMetric expectedMetric = expectedLocatorMetricMap.get(locator);
            MetricData metricData =
                    astyanaxMetricsRW.getDatapointsForRange (locator,
                            getRangeFromMinAgoToNow(5),
                            Granularity.FULL);

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            Assert.assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

            Assert.assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                    pointMap.get(expectedMetric.getCollectionTime())));

            Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
            Assert.assertTrue(String.format("locator %s data is the same", locator), expectedMetric.getMetricValue().equals(point.getData()));

            Set<Locator> ingestedLocators = new HashSet<Locator>(){{ add(locator); }};
            Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
            assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));

            List<IMetric> ingestedDelayedMetrics = new ArrayList<IMetric>(){{ add(expectedLocatorMetricMap.get(locator)); }};
            Set<Locator> ingestedDelayedLocators = new HashSet<Locator>(){{ add(locator); }};
            Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(ingestedDelayedMetrics);
            assertTrue("Some of the ingested locator's missing from db", locatorsFromDBByShardAndSlot.containsAll(ingestedDelayedLocators));
        }

        @Test
        public void testSingleMetricDataToRollup() throws Exception {

            // write with datastax
            datastaxMetricsRW.insertMetrics(expectedLocatorMetricMap.values());

            // pick first locator from input metrics, read with Astyanax.getDataToRollup
            Locator locator = expectedLocatorMetricMap.keySet().iterator().next();
            IMetric expectedMetric = expectedLocatorMetricMap.get(locator);
            Points points =
                    astyanaxMetricsRW.getDataToRollup(locator,
                            expectedMetric.getRollupType(),
                            getRangeFromMinAgoToNow(5),
                            CassandraModel.CF_METRICS_PREAGGREGATED_FULL_NAME);

            Map<Long, Points.Point> pointMap = points.getPoints();
            Assert.assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

            Assert.assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                    pointMap.get(expectedMetric.getCollectionTime())));

            Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
            Assert.assertTrue(String.format("locator %s data is the same", locator), expectedMetric.getMetricValue().equals(point.getData()));

        }

    }

    /**
     * A class that test cross-driver data correctness. This one has all the data
     * written using Datastax and read using Astyanax.
     *
     */
    @RunWith(JUnitParamsRunner.class)
    public static class WriteAstyanaxReadDatastax extends PreaggregatedMetricsRWIntegrationTest {

        @Test
        @Parameters(method = "getGranularitiesToTest")
        public void testMultiMetricsDatapointsRange(Granularity granularity) throws Exception {

            // write with astyanax
            astyanaxMetricsRW.insertMetrics(expectedLocatorMetricMap.values(), granularity);

            List<Locator> locators = new ArrayList<Locator>() {{
                addAll(expectedLocatorMetricMap.keySet());
            }};
            // read with datastax
            Map<Locator, MetricData> results = datastaxMetricsRW.getDatapointsForRange(
                    locators,
                    getRangeFromMinAgoToNow(5),
                    granularity);

            Assert.assertEquals("number of locators", expectedLocatorMetricMap.keySet().size(), results.keySet().size());

            for ( Map.Entry<Locator, IMetric> entry : expectedLocatorMetricMap.entrySet() ) {
                Locator locator = entry.getKey();

                MetricData metricData = results.get(locator);
                Assert.assertNotNull(String.format("metric data for locator %s exists", locator), metricData);

                Points points = metricData.getData();
                Map<Long, Points.Point> pointMap = points.getPoints();
                Assert.assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

                IMetric expectedMetric = entry.getValue();
                Assert.assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                        pointMap.get(expectedMetric.getCollectionTime())));

                Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
                Assert.assertTrue(String.format("locator %s data is the same", locator), expectedMetric.getMetricValue().equals(point.getData()));
            }

            Set<Locator> ingestedLocators = expectedLocatorMetricMap.keySet();
            Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
            assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));

            Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(new ArrayList<IMetric>(expectedLocatorMetricMap.values()));
            assertTrue("Locators which are not delayed identified as delayed", Collections.disjoint(locatorsFromDBByShardAndSlot, locators));
        }

        @Test
        @Parameters(method = "getGranularitiesToTest")
        public void testMultiMetricsDatapointsRangeWithDelayedMetrics(Granularity granularity) throws Exception {

            Clock clock = mock(Clock.class);
            final Locator locator1 = expectedLocatorMetricMap.keySet().iterator().next();
            when(clock.now()).thenReturn(new Instant(expectedLocatorMetricMap.get(locator1).getCollectionTime() + MAX_AGE_ALLOWED + 1000));

            // write with astyanax
            APreaggregatedMetricsRW astyanaxMetricsRW1 = new APreaggregatedMetricsRW(true, clock);
            astyanaxMetricsRW1.insertMetrics(expectedLocatorMetricMap.values(), granularity);

            List<Locator> locators = new ArrayList<Locator>() {{
                addAll(expectedLocatorMetricMap.keySet());
            }};
            // read with datastax
            Map<Locator, MetricData> results = datastaxMetricsRW.getDatapointsForRange(
                    locators,
                    getRangeFromMinAgoToNow(5),
                    granularity);

            Assert.assertEquals("number of locators", expectedLocatorMetricMap.keySet().size(), results.keySet().size());

            for ( Map.Entry<Locator, IMetric> entry : expectedLocatorMetricMap.entrySet() ) {
                Locator locator = entry.getKey();

                MetricData metricData = results.get(locator);
                Assert.assertNotNull(String.format("metric data for locator %s exists", locator), metricData);

                Points points = metricData.getData();
                Map<Long, Points.Point> pointMap = points.getPoints();
                Assert.assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

                IMetric expectedMetric = entry.getValue();
                Assert.assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                        pointMap.get(expectedMetric.getCollectionTime())));

                Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
                Assert.assertTrue(String.format("locator %s data is the same", locator), expectedMetric.getMetricValue().equals(point.getData()));
            }

            Set<Locator> ingestedLocators = expectedLocatorMetricMap.keySet();
            Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
            assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));

            Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(new ArrayList<IMetric>(expectedLocatorMetricMap.values()));
            locatorsFromDBByShardAndSlot.retainAll(ingestedLocators);
            assertEquals("Locators which are not delayed identified as delayed", locators.size(), locatorsFromDBByShardAndSlot.size());
        }

        @Test
        @Parameters(method = "getGranularitiesToTest")
        public void testInsertRollups(Granularity granularity) throws Exception {

            // write with astyanax
            List<SingleRollupWriteContext> writeContexts = toWriteContext(expectedLocatorMetricMap, granularity);
            astyanaxMetricsRW.insertRollups(writeContexts);

            // read with datastax
            List<Locator> locators = new ArrayList<Locator>() {{
                addAll(expectedLocatorMetricMap.keySet());
            }};
            // read with datastax
            Map<Locator, MetricData> results = datastaxMetricsRW.getDatapointsForRange(
                    locators,
                    getRangeFromMinAgoToNow(5),
                    granularity);

            Assert.assertEquals("number of locators", expectedLocatorMetricMap.keySet().size(), results.keySet().size());

            for ( Map.Entry<Locator, IMetric> entry : expectedLocatorMetricMap.entrySet() ) {
                Locator locator = entry.getKey();

                MetricData metricData = results.get(locator);
                Assert.assertNotNull(String.format("metric data for locator %s exists", locator), metricData);

                Points points = metricData.getData();
                Map<Long, Points.Point> pointMap = points.getPoints();
                Assert.assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

                IMetric expectedMetric = entry.getValue();
                Assert.assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                        pointMap.get(expectedMetric.getCollectionTime())));

                Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
                Assert.assertTrue(String.format("locator %s data is the same", locator), expectedMetric.getMetricValue().equals(point.getData()));
            }
        }

        @Test
        public void testSingleMetricDatapointForRange() throws Exception {

            // write with astyanax
            astyanaxMetricsRW.insertMetrics(expectedLocatorMetricMap.values());

            // pick first locator from input metrics, read with Astyanax.getDataToRollup
            final Locator locator = expectedLocatorMetricMap.keySet().iterator().next();
            IMetric expectedMetric = expectedLocatorMetricMap.get(locator);
            MetricData metricData =
                    datastaxMetricsRW.getDatapointsForRange (locator,
                            getRangeFromMinAgoToNow(5),
                            Granularity.FULL);

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            Assert.assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

            Assert.assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                    pointMap.get(expectedMetric.getCollectionTime())));

            Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
            Assert.assertTrue(String.format("locator %s data is the same", locator), expectedMetric.getMetricValue().equals(point.getData()));

            Set<Locator> ingestedLocators = new HashSet<Locator>(){{ add(locator); }};
            Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
            assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));

            List<IMetric> ingestedDelayedMetrics = new ArrayList<IMetric>(){{ add(expectedLocatorMetricMap.get(locator)); }};
            Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(ingestedDelayedMetrics);
            assertEquals("Locators which are not delayed identified as delayed", 0, locatorsFromDBByShardAndSlot.size());
        }

        @Test
        public void testSingleMetricDatapointForRangeWithDelayedMetrics() throws Exception {

            Clock clock = mock(Clock.class);
            final Locator locator = expectedLocatorMetricMap.keySet().iterator().next();
            when(clock.now()).thenReturn(new Instant(expectedLocatorMetricMap.get(locator).getCollectionTime() + MAX_AGE_ALLOWED + 1));

            // write with astyanax
            APreaggregatedMetricsRW astyanaxMetricsRW1 = new APreaggregatedMetricsRW(true, clock);
            astyanaxMetricsRW1.insertMetrics(expectedLocatorMetricMap.values());

            // pick first locator from input metrics, read with Astyanax.getDataToRollup
            IMetric expectedMetric = expectedLocatorMetricMap.get(locator);
            MetricData metricData =
                    datastaxMetricsRW.getDatapointsForRange (locator,
                            getRangeFromMinAgoToNow(5),
                            Granularity.FULL);

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            Assert.assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

            Assert.assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                    pointMap.get(expectedMetric.getCollectionTime())));

            Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
            Assert.assertTrue(String.format("locator %s data is the same", locator), expectedMetric.getMetricValue().equals(point.getData()));

            Set<Locator> ingestedLocators = new HashSet<Locator>(){{ add(locator); }};
            Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
            assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));

            List<IMetric> ingestedDelayedMetrics = new ArrayList<IMetric>(){{ add(expectedLocatorMetricMap.get(locator)); }};
            Set<Locator> ingestedDelayedLocators = new HashSet<Locator>(){{ add(locator); }};
            Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(ingestedDelayedMetrics);
            assertTrue("Some of the ingested locator's missing from db", locatorsFromDBByShardAndSlot.containsAll(ingestedDelayedLocators));
        }

        @Test
        public void testSingleMetricDataToRollup() throws Exception {

            // write with astyanax
            astyanaxMetricsRW.insertMetrics(expectedLocatorMetricMap.values());

            // pick first locator from input metrics, read with datastaxRW.getDataToRollup
            Locator locator = expectedLocatorMetricMap.keySet().iterator().next();
            IMetric expectedMetric = expectedLocatorMetricMap.get(locator);
            Points points =
                    datastaxMetricsRW.getDataToRollup(locator,
                            expectedMetric.getRollupType(),
                            getRangeFromMinAgoToNow(5),
                            CassandraModel.CF_METRICS_PREAGGREGATED_FULL_NAME);

            Map<Long, Points.Point> pointMap = points.getPoints();
            Assert.assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

            Assert.assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                    pointMap.get(expectedMetric.getCollectionTime())));

            Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
            Assert.assertTrue(String.format("locator %s data is the same", locator), expectedMetric.getMetricValue().equals(point.getData()));

        }
    }

    @RunWith(JUnitParamsRunner.class)
    public static class OtherFunctionalities extends PreaggregatedMetricsRWIntegrationTest {

        @Test
        @Parameters(method="getGranularitiesToTest")
        public void testSingleMetricTtlWorks(Granularity granularity) throws Exception {

            // pick first locator from input metrics
            Locator locator = expectedLocatorMetricMap.keySet().iterator().next();
            IMetric expectedMetric = expectedLocatorMetricMap.get(locator);

            // put it, with TTL 2 seconds
            expectedMetric.setTtlInSeconds(2);
            datastaxMetricsRW.insertMetrics(Lists.newArrayList(expectedMetric), granularity);

            // read it quickly.
            Points<BluefloodTimerRollup> points =
                    datastaxMetricsRW.getDataToRollup(locator,
                            expectedMetric.getRollupType(),
                            getRangeFromMinAgoToNow(5),
                            CassandraModel.getPreaggregatedColumnFamilyName(granularity));
            Assert.assertEquals("number of points read before TTL", 1, points.getPoints().size());

            // let it time out.
            Thread.sleep(2000);

            // ensure it is gone.
            points = datastaxMetricsRW.getDataToRollup(locator,
                    expectedMetric.getRollupType(),
                    getRangeFromMinAgoToNow(5),
                    CassandraModel.getPreaggregatedColumnFamilyName(granularity));
            Assert.assertEquals("number of points read after TTL", 0, points.getPoints().size());
        }

        @Test
        public void testHigherGranReadWrite() throws Exception {

            // pick a granularity
            Granularity granularity = Granularity.MIN_60;

            // insert metric
            datastaxMetricsRW.insertMetrics(Lists.newArrayList(timerMetric), granularity);

            // read the raw data.
            Points<BluefloodTimerRollup> points =
                    datastaxMetricsRW.getDataToRollup(timerMetric.getLocator(),
                            timerMetric.getRollupType(),
                            getRangeFromMinAgoToNow(5),
                            CassandraModel.getPreaggregatedColumnFamilyName(granularity));
            Assert.assertEquals("number of points read", 1, points.getPoints().size());

            // create the rollup
            final BluefloodTimerRollup rollup = BluefloodTimerRollup.buildRollupFromTimerRollups(points);
            // should be the same as simpletimerRollup   Assert.assertEquals(timerRollup, rollup);

            // assemble it into points, but give it a new timestamp.
            points = new Points<BluefloodTimerRollup>() {{
                add(new Point<BluefloodTimerRollup>(timerMetric.getCollectionTime(), rollup));
            }};
            Collection<IMetric> toWrite = toIMetricsCollection(timerMetric.getLocator(), points);
            datastaxMetricsRW.insertMetrics(toWrite, granularity.coarser());

            // we should be able to read that now.
            Points<BluefloodTimerRollup> pointsCoarser =
                    datastaxMetricsRW.getDataToRollup(
                            timerMetric.getLocator(),
                            RollupType.TIMER,
                            getRangeFromMinAgoToNow(5),
                            CassandraModel.getPreaggregatedColumnFamilyName(granularity.coarser()));

            Assert.assertEquals("number of points read in coarser gran", 1, pointsCoarser.getPoints().size());

            BluefloodTimerRollup rollupCoarser = pointsCoarser.getPoints().values().iterator().next().getData();
            // rollups should be identical since one is just a coarse rollup of the other.
            Assert.assertEquals("rollup read in coarser gran is the same", rollup, rollupCoarser);
        }

        @Test
        public void testLocatorWritten() throws Exception {

            // insert metrics using datastax
            datastaxMetricsRW.insertMetrics(expectedLocatorMetricMap.values());

            LocatorIO locatorIO = IOContainer.fromConfig().getLocatorIO();
            for ( Locator locator : expectedLocatorMetricMap.keySet() ) {
                long shard = Util.getShard(locator.toString());
                Collection<Locator> locators = locatorIO.getLocators(shard);
                Assert.assertTrue(String.format("locator %s should exist", locator), locators.contains(locator));
            }
        }

    }
}
