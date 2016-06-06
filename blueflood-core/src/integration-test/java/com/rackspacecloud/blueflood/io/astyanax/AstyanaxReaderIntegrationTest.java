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

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.io.ExcessEnumIO;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

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
    public void testCanReadString() throws Exception {
        Metric metric = writeMetric("string_metric", "version 1.0.43342346");
        final Locator locator = metric.getLocator();

        AstyanaxReader reader = AstyanaxReader.getInstance();
        MetricData res = reader.getDatapointsForRange(locator, new Range(metric.getCollectionTime() - 100000,
                metric.getCollectionTime() + 100000), Granularity.FULL);
        Assert.assertTrue(res.getData().getPoints().size() > 0);
    }

    @Test
    public void testCanReadExcessEnumMetrics() throws Exception {
        Locator loc1 = Locator.createLocatorFromPathComponents("acOne", "ent", "ch", "mz", "met");
        Locator loc2 = Locator.createLocatorFromPathComponents("acTwo", "ent", "ch", "mz", "met");
        Set<Locator> excessEnumList = new HashSet<Locator>();
        excessEnumList.add(loc1);
        excessEnumList.add(loc2);

        ExcessEnumIO excessEnumIO = new AExcessEnumIO();
        excessEnumIO.insertExcessEnumMetric(loc1);
        excessEnumIO.insertExcessEnumMetric(loc2);
        Set<Locator> newExcessEnumList = excessEnumIO.getExcessEnumMetrics();
        Assert.assertEquals(excessEnumList, newExcessEnumList);
    }

    @Test
    public void testCanReadEnumValuesByOneLocator() throws Exception {
        List<Locator> locatorList = new ArrayList<Locator>();
        IMetric metric = astyanaxWriteEnumMetric("enum_metric1", "333333");
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), null);
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.ENUM.toString());
        locatorList.add(metric.getLocator());

        // Test batch reads
        AstyanaxReader reader = AstyanaxReader.getInstance();
        MetricData results = reader.getDatapointsForRange(metric.getLocator(), new Range(metric.getCollectionTime() - 100000,
                metric.getCollectionTime() + 100000), Granularity.FULL);

        Assert.assertEquals(1, results.getData().getPoints().size());
        Points points = results.getData();
        Map<Long, Points.Point<BluefloodEnumRollup>> actualData = points.getPoints();
        verifyPointsData(actualData);
    }

    @Test
    public void testCanReadEnumValuesByMultipleLocators() throws Exception {
        List<Locator> locatorList = new ArrayList<Locator>();
        IMetric metric = astyanaxWriteEnumMetric("enum_metric2", "333333");
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), null);
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.ENUM.toString());
        locatorList.add(metric.getLocator());

        IMetric metric1 = astyanaxWriteEnumMetric("enum_metric3", "333333");
        MetadataCache.getInstance().put(metric1.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), null);
        MetadataCache.getInstance().put(metric1.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.ENUM.toString());
        locatorList.add(metric1.getLocator());

        AstyanaxReader reader = AstyanaxReader.getInstance();
        Map<Locator, MetricData> results = reader.getDatapointsForRange(locatorList, new Range(metric.getCollectionTime() - 100000,
                metric.getCollectionTime() + 100000), Granularity.FULL);

        Assert.assertEquals(2, results.size());

        for (Locator locator: results.keySet()) {
            Points points = results.get(locator).getData();
            Map<Long, Points.Point<BluefloodEnumRollup>> actualData = points.getPoints();
            Assert.assertEquals(1, actualData.size());
            verifyPointsData(actualData);
        }
    }

    @Test
    public void test_ReadEnumValues_MultipleLocators_OneLocatorFaulty() throws Exception {
        List<Locator> locatorList = new ArrayList<Locator>();
        IMetric metric = astyanaxWriteEnumMetric("enum_metric2", "333333");
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), null);
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.ENUM.toString());
        locatorList.add(metric.getLocator());

        IMetric metric1 = astyanaxWriteEnumMetric("enum_metric3", "333333");
        MetadataCache.getInstance().put(metric1.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), null);
        MetadataCache.getInstance().put(metric1.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.ENUM.toString());
        locatorList.add(metric1.getLocator());

        Locator faultyLocator = Locator.createLocatorFromPathComponents("333333", "faulty_enum_name");
        locatorList.add(faultyLocator);

        AstyanaxReader reader = AstyanaxReader.getInstance();
        Map<Locator, MetricData> results = reader.getDatapointsForRange(locatorList, new Range(metric.getCollectionTime() - 100000,
                metric.getCollectionTime() + 100000), Granularity.FULL);

        Assert.assertEquals(3, results.size());

        for (Locator locator: results.keySet()) {
            Points points = results.get(locator).getData();
            Map<Long, Points.Point<BluefloodEnumRollup>> actualData = points.getPoints();
            if (!locator.equals(faultyLocator)) {
                Assert.assertEquals(1, actualData.size());
                verifyPointsData(actualData);
            }
            else {
                Assert.assertEquals(0, actualData.size());
            }
        }
    }

    @Test
    public void testCanReadMixedEnumAndRegularMetrics() throws Exception {
        Set<Locator> enumLocatorsSet = new HashSet<Locator>();
        List<Locator> locatorList = new ArrayList<Locator>();
        IMetric metric = astyanaxWriteEnumMetric("enum_metric3", "333333");
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), null);
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.ENUM.toString());
        locatorList.add(metric.getLocator());
        enumLocatorsSet.add(metric.getLocator());

        IMetric metric1 = astyanaxWriteEnumMetric("enum_metric4", "333333");
        MetadataCache.getInstance().put(metric1.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), null);
        MetadataCache.getInstance().put(metric1.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.ENUM.toString());
        locatorList.add(metric1.getLocator());
        enumLocatorsSet.add(metric1.getLocator());

        metric = writeMetric("int_metric1", 45);
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), DataType.NUMERIC.toString());
        locatorList.add(metric.getLocator());

        metric = writeMetric("long_metric1", 67L);
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), DataType.NUMERIC.toString());
        locatorList.add(metric.getLocator());

        AstyanaxReader reader = AstyanaxReader.getInstance();
        Map<Locator, MetricData> results = reader.getDatapointsForRange(locatorList, new Range(metric.getCollectionTime() - 100000,
                metric.getCollectionTime() + 100000), Granularity.FULL);

        Assert.assertEquals(4, results.size());

        for (Locator locator: enumLocatorsSet) {
            Points points = results.get(locator).getData();
            Map<Long, Points.Point<BluefloodEnumRollup>> actualData = points.getPoints();
            Assert.assertEquals(1, actualData.size());
            verifyPointsData(actualData);
        }
    }

    @Test
    public void testBatchedReads() throws Exception {
        // Write metrics and also persist their types.
        List<Locator> locatorList = new ArrayList<Locator>();
        Metric metric = writeMetric("string_metric", "version 1.0.43342346");
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), DataType.STRING.toString());
        locatorList.add(metric.getLocator());

        metric = writeMetric("int_metric", 45);
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), DataType.NUMERIC.toString());
        locatorList.add(metric.getLocator());

        metric = writeMetric("long_metric", 67L);
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), DataType.NUMERIC.toString());
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
        Metric metric = writeMetric("string_metric", "version 1.0.43342346");
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), DataType.STRING.toString());
        locatorList.add(metric.getLocator());

        metric = writeMetric("int_metric", 45);
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
    public void test_StringMetrics_WithoutMetadata_NotRetrieved() throws Exception {
        List<Locator> locatorList = new ArrayList<Locator>();
        Metric metric = writeMetric("string_metric_1", "version 1.0.43342346");
        locatorList.add(metric.getLocator());

        // Test batch reads
        AstyanaxReader reader = AstyanaxReader.getInstance();
        Map<Locator, MetricData> results = reader.getDatapointsForRange(locatorList, new Range(metric.getCollectionTime() - 100000,
                metric.getCollectionTime() + 100000), Granularity.FULL);

        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.get(metric.getLocator()).getData().isEmpty());
    }

    @Test
    public void testNullRollupType_DoesNotReturn_StringOrBooleanSerializers() {
        AstyanaxReader reader = AstyanaxReader.getInstance();

        AbstractSerializer serializer = reader.serializerFor(null, DataType.INT, Granularity.MIN_5);

        Assert.assertTrue(serializer != null);
        Assert.assertFalse(serializer instanceof StringSerializer);
        Assert.assertFalse(serializer instanceof BooleanSerializer);
    }

    @Test
    public void testNullDataType_DoesNotReturn_StringOrBooleanSerializers() {
        AstyanaxReader reader = AstyanaxReader.getInstance();

        AbstractSerializer serializer = reader.serializerFor(null, null, Granularity.MIN_5);

        Assert.assertTrue(serializer != null);
        Assert.assertFalse(serializer instanceof StringSerializer);
        Assert.assertFalse(serializer instanceof BooleanSerializer);
    }

    private void verifyPointsData(Map<Long, Points.Point<BluefloodEnumRollup>> actualData) {
        for (Long timestamp : actualData.keySet()) {
            BluefloodEnumRollup er = actualData.get(timestamp).getData();
            Assert.assertEquals(1, er.getStringEnumValuesWithCounts().size());
            verifyRollupValues(er);
        }
    }

    private void verifyRollupValues(BluefloodEnumRollup er) {
        for (String value : er.getStringEnumValuesWithCounts().keySet()) {
            Assert.assertTrue(value.contains("enumValue"));
            Assert.assertEquals(1L, (long) er.getStringEnumValuesWithCounts().get(value));
        }
    }
}
