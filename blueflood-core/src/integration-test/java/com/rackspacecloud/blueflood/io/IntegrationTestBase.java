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

package com.rackspacecloud.blueflood.io;

import com.google.common.cache.Cache;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.serializers.StringSerializer;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxIO;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxReader;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxRowCounterFunction;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxWriter;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.io.CassandraModel.MetricColumnFamily;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.internal.util.reflection.Whitebox;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class IntegrationTestBase {

    public static class AstyanaxTester extends AstyanaxIO {
        // This is kind of gross and has serious room for improvement.
        protected void truncate(String cf) {
            int tries = 3;
            while (tries-- > 0) {
                try {
                    getKeyspace().truncateColumnFamily(cf);
                } catch (ConnectionException ex) {
                    System.err.println("Connection problem, yo. remaining tries: " + tries + " " + ex.getMessage());
                    try { Thread.sleep(1000L); } catch (Exception ewww) {}
                }
            }
        }

        protected final void assertNumberOfRows(String cf, long rows) throws Exception {
            ColumnFamily<String, String> columnFamily = ColumnFamily.newColumnFamily(cf, StringSerializer.get(), StringSerializer.get());
            AstyanaxRowCounterFunction<String, String> rowCounter = new AstyanaxRowCounterFunction<String, String>();
            boolean result = new AllRowsReader.Builder<String, String>(getKeyspace(), columnFamily)
                    .withColumnRange(null, null, false, 0)
                    .forEachRow(rowCounter)
                    .build()
                    .call();
            Assert.assertEquals(rows, rowCounter.getCount());
        }

        public ColumnFamily<Locator, Long> getStringCF() {
            return CassandraModel.CF_METRICS_STRING;
        }

        public ColumnFamily<Locator, Long> getFullCF() {
            return CassandraModel.CF_METRICS_FULL;
        }

        public ColumnFamily<Long, Locator> getLocatorCF() {
            return CassandraModel.CF_METRICS_LOCATOR;
        }

        public MutationBatch createMutationBatch() {
            return getKeyspace().prepareMutationBatch();
        }
    }

    private static final char[] STRING_SEEDS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890_".toCharArray();
    protected static final Random RAND = new Random(System.currentTimeMillis());
    protected static final ConcurrentHashMap<Locator, String> locatorToUnitMap = new ConcurrentHashMap<Locator, String>();
    protected static final List<String> enumValueList = Arrays.asList("A", "B", "C", "D", "E");

    @Before
    public void setUp() throws Exception {
        // really short lived connections for tests!
        AstyanaxTester truncator = new AstyanaxTester();
        for (ColumnFamily cf : CassandraModel.getAllColumnFamilies())
            truncator.truncate(cf.getName());
    }
    
    @After
    public void clearInterruptedThreads() throws Exception {
        // clear all interrupts! Why do we do this? The best I can come up with is that the test harness (junit) is
        // interrupting threads. One test in particular is very bad about this: RollupRunnableIntegrationTest.
        // Nothing in that test looks particularly condemnable other than the use of Metrics timers in the rollup
        // itself.  Anyway... this should clear up the travis build failures.
        //
        // Debugging this was an exceptional pain in the neck.  It turns out that there can be a fair amount of time
        // between when a thread is interrupted and an InterruptedException gets thrown. Minutes in our case. This is
        // because the AstyanaxWriter singleton keeps its threadpools between test invocations.
        //
        // The semantics of Thread.interrupt() are such that calling it only sets an interrupt flag to true, but doesn't
        // really interrupt the thread.  Subsequent calls to Thread.sleep() end up throwing the exception because the
        // thread is in an interrupted state.
        Method clearInterruptPrivate = Thread.class.getDeclaredMethod("isInterrupted", boolean.class);
        clearInterruptPrivate.setAccessible(true);
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isInterrupted()) {
                System.out.println(String.format("Clearing interrupted thread: " + thread.getName()));
                clearInterruptPrivate.invoke(thread, true);
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        // meh
    }

    /**
     * create a number of test locators of numTenants x numMetricNames combination
     * @param numTenants
     * @param numMetricNames
     * @return list of locators for testing
     */
    protected List<Locator> generateTestLocators(String tenantIdPrefix, int numTenants, String metricNamePrefix, int numMetricNames) {

        List<Locator> locators = new ArrayList<Locator>();
        for (int i = 1; i <= numTenants; i++) {
            for (int j = 1; j <= numMetricNames; j++) {
                locators.add(Locator.createLocatorFromPathComponents(tenantIdPrefix + i, metricNamePrefix + j));
            }
        }

        return locators;
    }

    protected Metric writeMetric(String name, Object value) throws Exception {
        final List<Metric> metrics = new ArrayList<Metric>();
        final Locator locator = Locator.createLocatorFromPathComponents("acctId", name);
        Metric metric = new Metric(locator, value, System.currentTimeMillis(),
                new TimeValue(1, TimeUnit.DAYS), "unknown");
        metrics.add(metric);
        AstyanaxWriter.getInstance().insertFull(metrics);
        Cache<String, Boolean> insertedLocators = (Cache<String, Boolean>) Whitebox.getInternalState(AstyanaxWriter.getInstance(), "insertedLocators");
        insertedLocators.invalidateAll();

        return metric;
    }

    protected IMetric writeEnumMetric(String name, String tenantid) throws Exception {
        final List<IMetric> metrics = new ArrayList<IMetric>();
        PreaggregatedMetric metric = getEnumMetric(name, tenantid, System.currentTimeMillis());
        metrics.add(metric);

        AstyanaxWriter.getInstance().insertMetrics(metrics, CassandraModel.CF_METRICS_PREAGGREGATED_FULL);

        Cache<String, Boolean> insertedLocators = (Cache<String, Boolean>) Whitebox.getInternalState(AstyanaxWriter.getInstance(), "insertedLocators");
        insertedLocators.invalidateAll();

        return metric;
    }

    protected PreaggregatedMetric getEnumMetric(String name, String tenantid, long timestamp) {
        final Locator locator = Locator.createLocatorFromPathComponents(tenantid, name);
        return getEnumMetric(locator, timestamp);
    }

    protected PreaggregatedMetric getEnumMetric(Locator locator, long timestamp) {
        BluefloodEnumRollup rollup = new BluefloodEnumRollup().withEnumValue("enumValue"+pickAnEnumValue());
        return new PreaggregatedMetric(timestamp, locator, new TimeValue(1, TimeUnit.DAYS), rollup);
    }

    protected List<Metric> makeRandomIntMetrics(int count) {
        final String tenantId = "ac" + randString(8);
        List<Metric> metrics = new ArrayList<Metric>();
        final long now = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            final Locator locator = Locator.createLocatorFromPathComponents(tenantId, "met" + randString(8));
            metrics.add(getRandomIntMetric(locator, now - 10000000));
        }

        return metrics;
    }

    protected static String randString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.append( STRING_SEEDS[ RAND.nextInt( STRING_SEEDS.length ) ] );
        return sb.toString();
    }

    protected int getRandomIntMetricValue() {
        return RAND.nextInt();
    }

    protected String getRandomStringMetricValue() {
        return "str" + String.valueOf(getRandomIntMetricValue());
    }

    protected Metric getRandomIntMetric(final Locator locator, long timestamp) {
        locatorToUnitMap.putIfAbsent(locator, UNIT_ENUM.values()[new Random().nextInt(UNIT_ENUM.values().length)].unit);
        return new Metric(locator, getRandomIntMetricValue(), timestamp, new TimeValue(1, TimeUnit.DAYS), locatorToUnitMap.get(locator));
    }

    protected Metric getRandomIntMetricMaxValue(final Locator locator, long timestamp, int max) {
        locatorToUnitMap.putIfAbsent(locator, UNIT_ENUM.values()[new Random().nextInt(UNIT_ENUM.values().length)].unit);
        return new Metric(locator, RAND.nextInt( max ), timestamp, new TimeValue(1, TimeUnit.DAYS), locatorToUnitMap.get(locator));
    }

    protected Metric getRandomStringmetric(final Locator locator, long timestamp) {
        locatorToUnitMap.putIfAbsent(locator, UNIT_ENUM.UNKNOWN.unit);
        return new Metric(locator, getRandomStringMetricValue(), timestamp, new TimeValue(1, TimeUnit.DAYS), locatorToUnitMap.get(locator));
    }

    protected static <T> Metric makeMetric(final Locator locator, long timestamp, T value) {
        return new Metric(locator, value, timestamp, new TimeValue(1, TimeUnit.DAYS), "unknown");
    }

    protected String pickAnEnumValue() {
        int index = RAND.nextInt(enumValueList.size() - 1);
        return enumValueList.get(index);
    }

    private enum UNIT_ENUM {
        SECS("seconds"),
        MSECS("milliseconds"),
        BYTES("bytes"),
        KILOBYTES("kilobytes"),
        UNKNOWN("unknown");

        private String unit;

        private UNIT_ENUM(String unitValue) {
            this.unit = unitValue;
        }

        private String getUnit() {
            return unit;
        }
    }

    protected void generateRollups(Locator locator, long from, long to, Granularity destGranularity) throws Exception {
        if (destGranularity == Granularity.FULL) {
            throw new Exception("Can't roll up to FULL");
        }

        MetricColumnFamily destCF;
        ArrayList<SingleRollupWriteContext> writeContexts = new ArrayList<SingleRollupWriteContext>();
        for (Range range : Range.rangesForInterval(destGranularity, from, to)) {
            destCF = CassandraModel.getColumnFamily(BasicRollup.class, destGranularity);
            Points<SimpleNumber> input = AstyanaxReader.getInstance().getDataToRoll(SimpleNumber.class, locator, range,
                    CassandraModel.CF_METRICS_FULL);
            BasicRollup basicRollup = BasicRollup.buildRollupFromRawSamples(input);
            writeContexts.add(new SingleRollupWriteContext(basicRollup, locator, destGranularity, destCF, range.start));

            destCF = CassandraModel.getColumnFamily(HistogramRollup.class, destGranularity);
            HistogramRollup histogramRollup = HistogramRollup.buildRollupFromRawSamples(input);
            writeContexts.add(new SingleRollupWriteContext(histogramRollup, locator, destGranularity, destCF, range.start));
        }

        AstyanaxWriter.getInstance().insertRollups(writeContexts);
    }

    protected void generateEnumRollups(Locator locator, long from, long to, Granularity destGranularity) throws Exception {
        if (destGranularity == Granularity.FULL) {
            throw new Exception("Can't roll up to FULL");
        }

        MetricColumnFamily destCF;
        ArrayList<SingleRollupWriteContext> writeContexts = new ArrayList<SingleRollupWriteContext>();
        for (Range range : Range.rangesForInterval(destGranularity, from, to)) {
            destCF = CassandraModel.getColumnFamily(BluefloodEnumRollup.class, destGranularity);
            Points<BluefloodEnumRollup> input = AstyanaxReader.getInstance().getDataToRoll(BluefloodEnumRollup.class, locator, range,
                    CassandraModel.CF_METRICS_PREAGGREGATED_FULL);
            BluefloodEnumRollup enumRollup = BluefloodEnumRollup.buildRollupFromEnumRollups(input);
            writeContexts.add(new SingleRollupWriteContext(enumRollup, locator, destGranularity, destCF, range.start));
        }

        AstyanaxWriter.getInstance().insertRollups(writeContexts);
    }

    private static final String TENANT1 = "11111";
    private static final String TENANT2 = "22222";
    private static final long   DELTA_MS = 2000;

    protected Range getRangeFromMinAgoToNow(int deltaMin) {
        // ask for 5 minutes back
        long now = System.currentTimeMillis();
        return new Range(now - (deltaMin*60*1000), now);
    }

    protected Map<Locator, List<IMetric>> generateEnumForTenants() throws Exception {
        Map<Locator, List<IMetric>> locatorToMetrics = new HashMap<Locator, List<IMetric>>();

        // setup some Enum data
        long startTime = System.currentTimeMillis();
        final Locator locator1 = Locator.createLocatorFromPathComponents(TENANT1, getClass().getSimpleName(), "my", "enum", "values");
        final Locator locator2 = Locator.createLocatorFromPathComponents(TENANT2, getClass().getSimpleName(), "my", "enum", "values");
        List<IMetric> metrics = new ArrayList<IMetric>();
        PreaggregatedMetric metric = getEnumMetric(locator1, startTime - DELTA_MS);
        metrics.add(metric);
        locatorToMetrics.put(locator1, metrics);

        metric = getEnumMetric(locator2, startTime);
        metrics = new ArrayList<IMetric>();
        metrics.add(metric);
        locatorToMetrics.put(locator2, metrics);

        // have to setup metadata so we correctly treat these metrics as
        // RollupType.ENUM
        MetadataCache.getInstance().put(locator1, MetricMetadata.TYPE.name().toLowerCase(), null);
        MetadataCache.getInstance().put(locator1, MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.ENUM.toString());
        MetadataCache.getInstance().put(locator2, MetricMetadata.TYPE.name().toLowerCase(), null);
        MetadataCache.getInstance().put(locator2, MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.ENUM.toString());
        return locatorToMetrics;
    }

    protected String getRandomTenantId() {

        return String.valueOf( RAND.nextInt( 5 ) );
    }

    protected void assertMetricDataEquals(MetricData expected, MetricData resultData) {

        Assert.assertEquals("metric data has the same type", expected.getType(), resultData.getType());
        Points expectedPoints = expected.getData();
        Points resultPoints = resultData.getData();
        Assert.assertEquals("metric data has same number of points", expectedPoints.getPoints().size(), resultPoints.getPoints().size());
        Assert.assertEquals("metric data dataClass is the same", expectedPoints.getDataClass(), resultPoints.getDataClass());
        Map<Long, Points.Point> expectedPointsMap = expectedPoints.getPoints();

        for ( Map.Entry<Long, Points.Point> entry : expectedPointsMap.entrySet() ) {
            Long timestamp = entry.getKey();
            Map<Long, Points.Point> map = resultPoints.getPoints();
            Points.Point resultPoint = map.get(timestamp);
            Assert.assertNotNull(String.format("result at timestamp %d exists", timestamp), resultPoint);
            Points.Point expectedPoint = entry.getValue();
            Assert.assertEquals(String.format("point at timestamp %d is the same", timestamp), expectedPoint, resultPoint);
        }

    }
}
