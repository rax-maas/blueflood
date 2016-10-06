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

package com.rackspacecloud.blueflood.io.datastax;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.DelayedLocatorIO;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.io.LocatorIO;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *  A class to test the behavior of DBasicRWIntegrationTest
 */
public class DBasicMetricsRWIntegrationTest extends IntegrationTestBase {

    protected LocatorIO locatorIO = new DLocatorIO();
    protected DelayedLocatorIO delayedLocatorIO = new DDelayedLocatorIO();

    @Test
    public void testStringMetricsIfSoConfiguredAreAlwaysDroppedForAllTenants() throws Exception {

        // the corresponding Astyanax version of this test is in
        // MetricsIntegrationTest.testStringMetricsIfSoConfiguredAreAlwaysDropped

        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);

        final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);
        MetadataCache.getInstance().put(locator, MetricMetadata.TYPE.name().toLowerCase(), DataType.STRING.toString());

        DBasicMetricsRW metricsRW = new DBasicMetricsRW(locatorIO, delayedLocatorIO, true, new ArrayList<String>(),
                false, new DefaultClockImpl());

        Set<Long> expectedTimestamps = new HashSet<Long>();
        // insert something every 30s for 5 mins.
        for (int i = 0; i < 10; i++) {
            final long curMillis = baseMillis + (i * 30000); // 30 seconds later.

            expectedTimestamps.add(curMillis);
            List<IMetric> metrics = new ArrayList<IMetric>();
            metrics.add(makeMetric(locator, curMillis, getRandomStringMetricValue()));
            metricsRW.insertMetrics(metrics);

        }

        // get back the cols that were written from start to stop.

        MetricData data = metricsRW.getDatapointsForRange(locator, new Range(baseMillis, lastMillis), Granularity.FULL);
        Set<Long> actualTimestamps = data.getData().getPoints().keySet();

        Assert.assertTrue(actualTimestamps.size() == 0);
    }

    @Test
    //In this test, string metrics are configured to be always dropped. So they are not persisted at all.
    public void testStringMetricsIfSoConfiguredAreNotDroppedForKeptTenantIds() throws Exception {

        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);
        final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);
        MetadataCache.getInstance().put(locator, MetricMetadata.TYPE.name().toLowerCase(), DataType.STRING.toString());

        List<String> keptTenants = new ArrayList<String>();
        keptTenants.add(locator.getTenantId());

        DBasicMetricsRW metricsRW = new DBasicMetricsRW(locatorIO, delayedLocatorIO, true, keptTenants,
                false, new DefaultClockImpl());

        Set<Long> expectedTimestamps = new HashSet<Long>();
        // insert something every 30s for 5 mins.
        for (int i = 0; i < 10; i++) {
            final long curMillis = baseMillis + (i * 30000); // 30 seconds later.

            expectedTimestamps.add(curMillis);
            List<IMetric> metrics = new ArrayList<IMetric>();
            metrics.add(makeMetric(locator, curMillis, getRandomStringMetricValue()));
            metricsRW.insertMetrics(metrics);
        }

        // get back the cols that were written from start to stop.

        MetricData data = metricsRW.getDatapointsForRange(locator, new Range(baseMillis, lastMillis), Granularity.FULL);
        Set<Long> actualTimestamps = data.getData().getPoints().keySet();

        Assert.assertEquals(expectedTimestamps, actualTimestamps);
    }

    @Test
    public void testStringMetricsIfSoConfiguredArePersistedAsExpected() throws Exception {
        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);

        final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);
        MetadataCache.getInstance().put(locator, MetricMetadata.TYPE.name().toLowerCase(), DataType.STRING.toString());

        DBasicMetricsRW metricsRW = new DBasicMetricsRW(locatorIO, delayedLocatorIO, false, new ArrayList<String>(),
                false, new DefaultClockImpl());

        Set<Long> expectedTimestamps = new HashSet<Long>();
        // insert something every 30s for 5 mins.
        for (int i = 0; i < 10; i++) {
            final long curMillis = baseMillis + (i * 30000); // 30 seconds later.

            expectedTimestamps.add(curMillis);
            List<IMetric> metrics = new ArrayList<IMetric>();
            metrics.add(makeMetric(locator, curMillis, getRandomStringMetricValue()));
            metricsRW.insertMetrics(metrics);
        }

        // get back the cols that were written from start to stop.

        MetricData data = metricsRW.getDatapointsForRange(locator, new Range(baseMillis, lastMillis), Granularity.FULL);
        Set<Long> actualTimestamps = data.getData().getPoints().keySet();

        Assert.assertEquals(expectedTimestamps, actualTimestamps);
    }

    @Test
    public void testStringMetricsWithSameValueAreNotPersisted() throws Exception {
        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);

        final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);
        MetadataCache.getInstance().put(locator, MetricMetadata.TYPE.name().toLowerCase(), DataType.STRING.toString());

        DBasicMetricsRW metricsRW = new DBasicMetricsRW(locatorIO, delayedLocatorIO, false, new ArrayList<String>(),
                false, new DefaultClockImpl());

        String sameValue = getRandomStringMetricValue();
        Set<Long> expectedTimestamps = new HashSet<Long>();
        // insert something every 30s for 5 mins.
        //value remains the same
        for (int i = 0; i < 10; i++) {
            final long curMillis = baseMillis + (i * 30000); // 30 seconds later.

            expectedTimestamps.add(curMillis);
            List<IMetric> metrics = new ArrayList<IMetric>();
            metrics.add(makeMetric(locator, curMillis, sameValue));
            metricsRW.insertMetrics(metrics);
        }

        // get back the cols that were written from start to stop.

        MetricData data = metricsRW.getDatapointsForRange(locator, new Range(baseMillis, lastMillis), Granularity.FULL);
        Set<Long> actualTimestamps = data.getData().getPoints().keySet();

        Assert.assertTrue(actualTimestamps.size() == 1);
        for (long ts : actualTimestamps) {
            Assert.assertEquals(ts, baseMillis);
            break;
        }
    }

    @Test
    public void testStringMetricsWithDifferentValuesArePersisted() throws Exception {
        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);

        final Locator locator  = Locator.createLocatorFromPathComponents(acctId, metricName);
        MetadataCache.getInstance().put(locator, MetricMetadata.TYPE.name().toLowerCase(), DataType.STRING.toString());
        String firstValue = getRandomStringMetricValue();
        String secondValue = getRandomStringMetricValue();

        DBasicMetricsRW metricsRW = new DBasicMetricsRW(locatorIO, delayedLocatorIO, false, new ArrayList<String>(),
                false, new DefaultClockImpl());

        Set<Long> expectedTimestamps = new HashSet<Long>();
        // insert something every 30s for 5 mins.
        //string metric value is alternated.
        for (int i = 0; i < 10; i++) {
            final long curMillis = baseMillis + (i * 30000); // 30 seconds later.
            String value = null;
            if (i % 2 == 0) {
                value = firstValue;
            }
            else {
                value = secondValue;
            }
            expectedTimestamps.add(curMillis);
            List<IMetric> metrics = new ArrayList<IMetric>();
            metrics.add(makeMetric(locator, curMillis, value));
            metricsRW.insertMetrics(metrics);
        }

        // get back the cols that were written from start to stop.

        MetricData data = metricsRW.getDatapointsForRange(locator, new Range(baseMillis, lastMillis),Granularity.FULL);
        Set<Long> actualTimestamps = data.getData().getPoints().keySet();
        Assert.assertEquals(expectedTimestamps, actualTimestamps);
    }

    @Test
    //Numeric value is always persisted.
    public void testNumericMetricsAreAlwaysPersisted() throws Exception {

        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);

        final Locator locator  = Locator.createLocatorFromPathComponents(acctId, metricName);

        DBasicMetricsRW metricsRW = new DBasicMetricsRW(locatorIO, delayedLocatorIO, false, new ArrayList<String>(),
                false, new DefaultClockImpl());

        int sameValue = getRandomIntMetricValue();
        Set<Long> expectedTimestamps = new HashSet<Long>();
        // insert something every 30s for 5 mins.
        //value of numeric metric remains the same, still it is always persisted
        for (int i = 0; i < 10; i++) {
            final long curMillis = baseMillis + (i * 30000); // 30 seconds later.
            expectedTimestamps.add(curMillis);
            List<IMetric> metrics = new ArrayList<IMetric>();
            metrics.add(makeMetric(locator, curMillis,sameValue));
            metricsRW.insertMetrics(metrics);
        }

        // get back the cols that were written from start to stop.

        Points<SimpleNumber> points = metricsRW.getDataToRollup(locator, RollupType.BF_BASIC, new Range(baseMillis, lastMillis),
                CassandraModel.getBasicColumnFamilyName(Granularity.FULL));
        Set<Long> actualTimestamps = points.getPoints().keySet();
        Assert.assertEquals(expectedTimestamps, actualTimestamps);
    }

    @Test
    //In this test, the same value is sent, and the metric is not persisted except for the first time.
    public void testBooleanMetricsWithSameValueAreNotPersisted() throws Exception {

        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);

        final Locator locator  = Locator.createLocatorFromPathComponents(acctId, metricName);
        MetadataCache.getInstance().put(locator, MetricMetadata.TYPE.name().toLowerCase(), DataType.BOOLEAN.toString());

        DBasicMetricsRW metricsRW = new DBasicMetricsRW(locatorIO, delayedLocatorIO, false, new ArrayList<String>(),
                false, new DefaultClockImpl());

        boolean sameValue = true;
        Set<Long> expectedTimestamps = new HashSet<Long>();
        // insert something every 30s for 5 mins.
        for (int i = 0; i < 10; i++) {
            final long curMillis = baseMillis + (i * 30000); // 30 seconds later.
            expectedTimestamps.add(curMillis);
            List<IMetric> metrics = new ArrayList<IMetric>();
            metrics.add(makeMetric(locator,curMillis,sameValue));
            metricsRW.insertMetrics(metrics);
        }

        // get back the cols that were written from start to stop.

        MetricData data = metricsRW.getDatapointsForRange(locator, new Range(baseMillis, lastMillis),Granularity.FULL);
        Set<Long> actualTimestamps = data.getData().getPoints().keySet();

        Assert.assertTrue(actualTimestamps.size() == 1);
        for(long ts : actualTimestamps) {
            Assert.assertEquals(ts, baseMillis);
            break;
        }
    }

    @Test
    //In this test, we alternately persist true and false. All the boolean metrics are persisted.
    public void testBooleanMetricsWithDifferentValuesArePersisted() throws Exception {

        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);

        final Locator locator  = Locator.createLocatorFromPathComponents(acctId, metricName);
        MetadataCache.getInstance().put(locator, MetricMetadata.TYPE.name().toLowerCase(), DataType.BOOLEAN.toString());

        DBasicMetricsRW metricsRW = new DBasicMetricsRW(locatorIO, delayedLocatorIO, false, new ArrayList<String>(),
                false, new DefaultClockImpl());

        Set<Long> expectedTimestamps = new HashSet<Long>();
        // insert something every 30s for 5 mins.
        for (int i = 0; i < 10; i++) {
            final long curMillis = baseMillis + (i * 30000); // 30 seconds later.
            boolean value;
            if (i % 2 == 0) {
                value = true;
            }
            else {
                value = false;
            }
            expectedTimestamps.add(curMillis);
            List<IMetric> metrics = new ArrayList<IMetric>();
            metrics.add(makeMetric(locator, curMillis, value));
            metricsRW.insertMetrics(metrics);
        }

        // get back the cols that were written from start to stop.

        MetricData data = metricsRW.getDatapointsForRange(locator, new Range(baseMillis, lastMillis),Granularity.FULL);
        Set<Long> actualTimestamps = data.getData().getPoints().keySet();
        Assert.assertEquals(expectedTimestamps, actualTimestamps);
    }

}
