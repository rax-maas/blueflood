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
package com.rackspacecloud.blueflood.io.astyanax;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.io.serializers.Serializers;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AstyanaxReaderIntegrationTest extends IntegrationTestBase {
    
    @Test
    public void testCanReadNumeric() throws Exception {
        Metric metric = writeMetric("long_metric", 74L);
        AstyanaxReader reader = AstyanaxReader.getInstance();

        final Locator locator = metric.getLocator();
        MetricData res = reader.getDatapointsForRange(locator, new Range(metric.getCollectionTime() - 100000,
                metric.getCollectionTime() + 100000), Granularity.FULL);
        int numPoints = res.getData().getPoints().size();
        Assert.assertTrue(numPoints > 0);

        // Test that the RangeBuilder is end-inclusive on the timestamp.
        res = reader.getDatapointsForRange(locator, new Range(metric.getCollectionTime() - 100000,
                metric.getCollectionTime()), Granularity.FULL);
        Assert.assertEquals(numPoints, res.getData().getPoints().size());
    }

    @Test
    public void testBatchedReads() throws Exception {
        // Write metrics and also persist their types.
        List<Locator> locatorList = new ArrayList<Locator>();

        Metric metric = writeMetric("int_metric", 45);
        locatorList.add(metric.getLocator());

        metric = writeMetric("long_metric", 67L);
        locatorList.add(metric.getLocator());

        // Test batch reads
        AstyanaxReader reader = AstyanaxReader.getInstance();
        Map<Locator, MetricData> results = reader.getDatapointsForRange(locatorList, new Range(metric.getCollectionTime() - 100000,
                metric.getCollectionTime() + 100000), Granularity.FULL);

        Assert.assertEquals(locatorList.size(), results.size());

        for (Locator locator : locatorList) {
            MetricData metrics = results.get(locator);
            Assert.assertEquals(1, metrics.getData().getPoints().size());
        }
    }

    @Test
    public void testCanRetrieveNumericMetricsEvenIfNoMetaDataStored() throws Exception {
        // Write metrics and also persist their types.
        List<Locator> locatorList = new ArrayList<Locator>();
        Metric metric = writeMetric("int_metric", 45);
        locatorList.add(metric.getLocator());

        metric = writeMetric("long_metric", 67L);
        locatorList.add(metric.getLocator());

        // Test batch reads
        AstyanaxReader reader = AstyanaxReader.getInstance();
        Map<Locator, MetricData> results = reader.getDatapointsForRange(locatorList, new Range(metric.getCollectionTime() - 100000,
                metric.getCollectionTime() + 100000), Granularity.FULL);

        Assert.assertEquals(locatorList.size(), results.size());

        for (Locator locator : locatorList) {
            MetricData metrics = results.get(locator);
            Assert.assertEquals(1, metrics.getData().getPoints().size());
        }
    }

    @Test
    public void testNullDataType_Return_BasicSerializers() {
        AstyanaxReader reader = AstyanaxReader.getInstance();

        Object serializer = reader.serializerFor(null, Granularity.MIN_5);

        Assert.assertTrue(serializer != null);
        Assert.assertTrue(serializer instanceof Serializers.BasicRollupSerializer);
    }
}
