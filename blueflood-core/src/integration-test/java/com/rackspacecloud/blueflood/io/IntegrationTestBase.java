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

import com.rackspacecloud.blueflood.cache.LocatorCache;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.datastax.DCassandraUtilsIO;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.io.CassandraModel.MetricColumnFamily;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.junit.After;
import org.junit.Before;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class IntegrationTestBase {

    private static final char[] STRING_SEEDS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890_".toCharArray();
    protected static final Random RAND = new Random(System.currentTimeMillis());
    protected static final ConcurrentHashMap<Locator, String> locatorToUnitMap = new ConcurrentHashMap<Locator, String>();

    @Before
    public void setUp() throws Exception {
        // really short lived connections for tests!
        CassandraUtilsIO cassandraUtilsIO = new DCassandraUtilsIO();
        for (String cf : CassandraModel.getAllColumnFamiliesNames())
            cassandraUtilsIO.truncateColumnFamily(cf);
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
        final List<IMetric> metrics = new ArrayList<IMetric>();
        final Locator locator = Locator.createLocatorFromPathComponents("acctId", name);
        Metric metric = new Metric(locator, value, System.currentTimeMillis(),
                new TimeValue(1, TimeUnit.DAYS), "unknown");
        metrics.add(metric);
        AstyanaxWriter.getInstance().insertFull(metrics, false, new DefaultClockImpl());
        LocatorCache.getInstance().resetInsertedLocatorsCache();

        return metric;
    }

    protected PreaggregatedMetric getGaugeMetric(String name, String tenantid, long timestamp) {
        final Locator locator = Locator.createLocatorFromPathComponents(tenantid, name);
        return getGaugeMetric(locator, timestamp);
    }

    protected PreaggregatedMetric getGaugeMetric(Locator locator, long timestamp) {
        BluefloodGaugeRollup rollup = new BluefloodGaugeRollup()
                .withLatest(timestamp, new Long(getRandomIntMetricValue()));
        return new PreaggregatedMetric(timestamp, locator, new TimeValue(1, TimeUnit.DAYS), rollup);
    }

    protected List<IMetric> makeRandomIntMetrics(int count) {
        final String tenantId = "ac" + randString(8);
        List<IMetric> metrics = new ArrayList<IMetric>();
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

        MetricsRW metricsRW = IOContainer.fromConfig().getBasicMetricsRW();
        MetricColumnFamily destCF;
        ArrayList<SingleRollupWriteContext> writeContexts = new ArrayList<SingleRollupWriteContext>();
        for (Range range : Range.rangesForInterval(destGranularity, from, to)) {
            destCF = CassandraModel.getColumnFamily(BasicRollup.class, destGranularity);
            Points<SimpleNumber> input = metricsRW.getDataToRollup(locator, RollupType.BF_BASIC, range,
                    CassandraModel.CF_METRICS_FULL_NAME);
            BasicRollup basicRollup = BasicRollup.buildRollupFromRawSamples(input);
            writeContexts.add(new SingleRollupWriteContext(basicRollup, locator, destGranularity, destCF, range.start));
        }

        metricsRW.insertRollups(writeContexts);
    }

    protected Range getRangeFromMinAgoToNow(int deltaMin) {
        // ask for 5 minutes back
        long now = System.currentTimeMillis();
        return new Range(now - (deltaMin*60*1000), now);
    }

    protected String getRandomTenantId() {

        return String.valueOf( RAND.nextInt( 5 ) );
    }
}
