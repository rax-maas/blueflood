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

import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.types.Rollup;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

/**
 * A class testing the behavior of Datastax's EnumIO.
 */
public class DEnumIOIntegrationTest extends IntegrationTestBase {

    protected Map<Locator, List<IMetric>> locatorToMetrics;
    protected DEnumIO dEnumIO = new DEnumIO();

    @Before
    public void generateEnum() throws Exception {
        locatorToMetrics = generateEnumForTenants();
    }

    @Test
    public void testWriteAndReadJoinConsistency() throws Exception {

        // write the metrics
        for (Locator locator : locatorToMetrics.keySet()) {
            List<IMetric> metrics = locatorToMetrics.get(locator);
            for (IMetric metric : metrics) {
                ResultSetFuture future = dEnumIO.putAsync(locator,
                                metric.getCollectionTime(),
                                (Rollup) metric.getMetricValue(),
                                Granularity.FULL,
                                metric.getTtlInSeconds());
                future.getUninterruptibly();
            }
        }

        // read them back
        // EnumIO has extra join in toLocatorTImestampRollup() that we want
        // to make sure the data is correct and consistent
        Range range = getRangeFromMinAgoToNow(5);
        for ( Locator locator : locatorToMetrics.keySet() ) {
            List<ResultSetFuture> results = dEnumIO.selectForLocatorAndRange(CassandraModel.CF_METRICS_PREAGGREGATED_FULL_NAME, locator, range);
            Assert.assertEquals("ResultSetFuture list size", 2, results.size());

            Table<Locator, Long, Rollup> locatorRollup = dEnumIO.toLocatorTimestampValue( results, locator, Granularity.FULL );
            Map<Long, Rollup> resultMap = locatorRollup.row(locator);

            List<IMetric> expectedMetrics = locatorToMetrics.get(locator);
            Assert.assertEquals("rollup size", expectedMetrics.size(), resultMap.size());

            IMetric expectedMetric = expectedMetrics.get(0);
            Rollup resultRollup = resultMap.get(expectedMetric.getCollectionTime());
            Assert.assertEquals("rollup object for locator " + locator, expectedMetric.getMetricValue(), resultRollup);
        }
    }
}
