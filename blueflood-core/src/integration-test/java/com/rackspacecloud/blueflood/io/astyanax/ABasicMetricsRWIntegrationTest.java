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

}
