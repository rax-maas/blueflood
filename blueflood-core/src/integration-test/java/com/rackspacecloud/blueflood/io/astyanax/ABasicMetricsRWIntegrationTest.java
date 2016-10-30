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

package com.rackspacecloud.blueflood.io.astyanax;

import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A class to test the behavior of ABasicRWIntegrationTest
 */
public class ABasicMetricsRWIntegrationTest extends IntegrationTestBase {

    @Test
    //In this test, string metrics are configured to be always dropped. So they are not persisted at all.
    public void testStringMetricsIfSoConfiguredAreAlwaysDropped() throws Exception {
        Boolean orgAreStringDropped = (Boolean) Whitebox.getInternalState(AstyanaxWriter.getInstance(), "areStringMetricsDropped");
        try {
            Whitebox.setInternalState(AstyanaxWriter.getInstance(), "areStringMetricsDropped", true);

            MetricsRW metricsRW = new ABasicMetricsRW(false, new DefaultClockImpl());

            final long baseMillis = 1333635148000L; // some point during 5 April 2012.
            long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
            final String acctId = "ac" + IntegrationTestBase.randString(8);
            final String metricName = "fooService,barServer," + randString(8);

            final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);

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
        } finally {
            Whitebox.setInternalState(AstyanaxWriter.getInstance(), "areStringMetricsDropped", orgAreStringDropped.booleanValue());
        }
    }

    @Test
    //In this test, string metrics are configured to be always dropped. So they are not persisted at all.
    public void testStringMetricsIfSoConfiguredAreNotDroppedForKeptTenantIds() throws Exception {
        Boolean orgAreStringDropped = (Boolean) Whitebox.getInternalState(AstyanaxWriter.getInstance(), "areStringMetricsDropped");
        try {
            Whitebox.setInternalState(AstyanaxWriter.getInstance(), "areStringMetricsDropped", true);

            MetricsRW metricsRW = new ABasicMetricsRW(false, new DefaultClockImpl());

            final long baseMillis = 1333635148000L; // some point during 5 April 2012.
            long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
            final String acctId = "ac" + IntegrationTestBase.randString(8);
            final String metricName = "fooService,barServer," + randString(8);

            final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);
            HashSet<String> keptTenants = new HashSet<String>();
            keptTenants.add(locator.getTenantId());

            Whitebox.setInternalState(AstyanaxWriter.getInstance(), "keptTenantIdsSet", keptTenants);

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
        } finally {
            Whitebox.setInternalState(AstyanaxWriter.getInstance(), "areStringMetricsDropped", orgAreStringDropped.booleanValue());
        }
    }

    @Test
    //In this test, string metrics are not configured to be dropped so they are persisted.
    public void testStringMetricsIfSoConfiguredArePersistedAsExpected() throws Exception {
        Boolean orgAreStringDropped = (Boolean) Whitebox.getInternalState(AstyanaxWriter.getInstance(), "areStringMetricsDropped");
        try {
            Whitebox.setInternalState(AstyanaxWriter.getInstance(), "areStringMetricsDropped", false);

            MetricsRW metricsRW = new ABasicMetricsRW(false, new DefaultClockImpl());

            final long baseMillis = 1333635148000L; // some point during 5 April 2012.
            long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
            final String acctId = "ac" + IntegrationTestBase.randString(8);
            final String metricName = "fooService,barServer," + randString(8);

            final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);

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
        } finally {
            Whitebox.setInternalState(AstyanaxWriter.getInstance(), "areStringMetricsDropped", orgAreStringDropped.booleanValue());
        }
    }

    @Test
    //In this test, we attempt to persist the same value of String Metric every single time. Only the first one is persisted.
    public void testStringMetricsWithSameValueAreNotPersisted() throws Exception {
        Boolean orgAreStringDropped = (Boolean) Whitebox.getInternalState(AstyanaxWriter.getInstance(), "areStringMetricsDropped");
        try {
            Whitebox.setInternalState(AstyanaxWriter.getInstance(), "areStringMetricsDropped", false);

            MetricsRW metricsRW = new ABasicMetricsRW(false, new DefaultClockImpl());

            final long baseMillis = 1333635148000L; // some point during 5 April 2012.
            long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
            final String acctId = "ac" + IntegrationTestBase.randString(8);
            final String metricName = "fooService,barServer," + randString(8);

            final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);
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
        } finally {
            Whitebox.setInternalState(AstyanaxWriter.getInstance(), "areStringMetricsDropped", orgAreStringDropped.booleanValue());
        }
    }

    @Test
    //In this case, we alternate between two values for a string metric. But since the string metric does not have the same value in two
    //consecutive writes, it's always persisted.
    public void testStringMetricsWithDifferentValuesArePersisted() throws Exception {

        MetricsRW metricsRW = new ABasicMetricsRW(false, new DefaultClockImpl());

        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);

        final Locator locator  = Locator.createLocatorFromPathComponents(acctId, metricName);
        String firstValue = getRandomStringMetricValue();
        String secondValue = getRandomStringMetricValue();

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

        MetricsRW metricsRW = new ABasicMetricsRW(false, new DefaultClockImpl());

        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);

        final Locator locator  = Locator.createLocatorFromPathComponents(acctId, metricName);
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

        MetricsRW metricsRW = new ABasicMetricsRW(false, new DefaultClockImpl());

        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);

        final Locator locator  = Locator.createLocatorFromPathComponents(acctId, metricName);
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

        MetricsRW metricsRW = new ABasicMetricsRW(false, new DefaultClockImpl());

        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        long lastMillis = baseMillis + (300 * 1000); // 300 seconds.
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);

        final Locator locator  = Locator.createLocatorFromPathComponents(acctId, metricName);

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
