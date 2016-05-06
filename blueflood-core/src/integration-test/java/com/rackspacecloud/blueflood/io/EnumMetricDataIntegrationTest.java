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

import com.rackspacecloud.blueflood.io.astyanax.AstyanaxReader;
import com.rackspacecloud.blueflood.io.datastax.DEnumIO;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * This is the tests for {@link EnumMetricData} class
 */
public class EnumMetricDataIntegrationTest extends IntegrationTestBase {

    private Map<Locator, List<IMetric>> locatorToMetrics;
    private static DEnumIO datastaxEnumIO;
    private static EnumMetricData enumMetricData;

    @BeforeClass
    public static void setup() {

        datastaxEnumIO = new DEnumIO();
        enumMetricData = new EnumMetricData(datastaxEnumIO);
    }

    @Before
    public void generateEnum() throws Exception {
        locatorToMetrics = generateEnumForTenants();
    }


    @Test
    public void metricDataReadWithDatastaxShouldBeSameAsAstyanax() throws Exception {

        // write using Datastax
        for (Map.Entry<Locator, List<IMetric>> entry : locatorToMetrics.entrySet()) {
            Locator locator = entry.getKey();
            List<IMetric> metrics = entry.getValue();
            for (IMetric metric : metrics) {
                datastaxEnumIO.putAsync(locator,
                        metric.getCollectionTime(),
                        (BluefloodEnumRollup) metric.getMetricValue(),
                        Granularity.FULL,
                        metric.getTtlInSeconds());
            }
        }

        // use one of the locator
        final Locator locator = locatorToMetrics.keySet().iterator().next();
        Range range = getRangeFromMinAgoToNow(5);
        MetricData metricData = enumMetricData.getEnumMetricDataForRange(locator, range, Granularity.FULL);

        // get expected using Astyanax
        List<Locator> locators = new ArrayList<Locator>() {{ add(locator); }};
        Map<Locator, MetricData> locatorMetricDataMap = AstyanaxReader.getInstance().getEnumMetricDataForRangeForLocatorList(locators, range, Granularity.FULL);
        MetricData expectedMetricData = locatorMetricDataMap.get(locator);
        assertMetricDataEquals(expectedMetricData, metricData);
    }

    @Test
    public void metricDataListReadWithDatastaxShouldBeSameAsAstyanax() throws Exception {

        // write using Datastax
        for (Map.Entry<Locator, List<IMetric>> entry : locatorToMetrics.entrySet()) {
            Locator locator = entry.getKey();
            List<IMetric> metrics = entry.getValue();
            for (IMetric metric : metrics) {
                datastaxEnumIO.putAsync(locator,
                        metric.getCollectionTime(),
                        (BluefloodEnumRollup) metric.getMetricValue(),
                        Granularity.FULL,
                        metric.getTtlInSeconds());
            }
        }
        // to apease Travis
        Thread.sleep(500);

        // use one of the locator
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(locatorToMetrics.keySet());
        }};
        Range range = getRangeFromMinAgoToNow(5);
        Map<Locator, MetricData> metricDataMap = enumMetricData.getEnumMetricDataForRangeForLocatorList(locators, range, Granularity.FULL);

        // get expected using Astyanax
        Map<Locator, MetricData> expectedMetricDataMap = AstyanaxReader.getInstance().getEnumMetricDataForRangeForLocatorList(locators, range, Granularity.FULL);

        Assert.assertEquals("locator to metricData map size", expectedMetricDataMap.size(), metricDataMap.size());

        for ( Locator locator : expectedMetricDataMap.keySet() ) {
            MetricData expectedMetricData = expectedMetricDataMap.get(locator);
            MetricData metricData = metricDataMap.get(locator);
            assertNotNull(String.format("metricData for locator%s exists", locator), metricData);
            assertMetricDataEquals(expectedMetricData, metricData);
        }
    }

}
