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

package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.utils.TimeValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

public class MetricTest {

    @Test
    public void testMetricType() {
        Locator locator = Locator.createLocatorFromPathComponents("tenantId", "metricName");

        Metric metric = new Metric(locator, "Foo", System.currentTimeMillis(), new TimeValue(5, TimeUnit.HOURS), "Unknown");
        Assert.assertEquals("S", metric.getType().toString());
        Assert.assertTrue(metric.getType().equals(Metric.Type.STRING));
        Assert.assertTrue("Metric should be string", metric.isString());
        Assert.assertTrue(Metric.Type.isKnownMetricType(metric.getType()));

        metric = new Metric(locator, 1234567L, System.currentTimeMillis(), new TimeValue(5, TimeUnit.HOURS), "Unknown");
        Assert.assertEquals("L", metric.getType().toString());
        Assert.assertTrue(metric.getType().equals(Metric.Type.LONG));
        Assert.assertTrue("Metric should be numeric", metric.isNumeric());
        Assert.assertTrue(Metric.Type.isKnownMetricType(metric.getType()));

        metric = new Metric(locator, 1234567.678, System.currentTimeMillis(), new TimeValue(5, TimeUnit.HOURS), "Unknown");
        Assert.assertEquals("D", metric.getType().toString());
        Assert.assertTrue(metric.getType().equals(Metric.Type.DOUBLE));
        Assert.assertTrue("Metric should be numeric", metric.isNumeric());
        Assert.assertTrue(Metric.Type.isKnownMetricType(metric.getType()));

        metric = new Metric(locator, 1234567, System.currentTimeMillis(), new TimeValue(5, TimeUnit.HOURS), "Unknown");
        Assert.assertEquals("I", metric.getType().toString());
        Assert.assertTrue(metric.getType().equals(Metric.Type.INT));
        Assert.assertTrue("Metric should be numeric", metric.isNumeric());
        Assert.assertTrue(Metric.Type.isKnownMetricType(metric.getType()));

        metric = new Metric(locator, false, System.currentTimeMillis(), new TimeValue(5, TimeUnit.HOURS), "Unknown");
        Assert.assertEquals("B", metric.getType().toString());
        Assert.assertTrue(metric.getType().equals(Metric.Type.BOOLEAN));
        Assert.assertTrue("Metric should be boolean", metric.isBoolean());
        Assert.assertTrue(Metric.Type.isKnownMetricType(metric.getType()));

        Metric.Type failType = new Metric.Type("X");
        Assert.assertFalse(Metric.Type.isKnownMetricType(failType));
    }

    @Test
    public void testTTL() {
        Locator locator = Locator.createLocatorFromPathComponents("tenantId", "metricName");
        Metric metric = new Metric(locator, "Foo", System.currentTimeMillis(), new TimeValue(5, TimeUnit.HOURS), "Unknown");

        try {
            metric.setTtl(new TimeValue(Long.MAX_VALUE, TimeUnit.SECONDS));
            fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RuntimeException);
        }
    }

    @Test
    public void testMetricValueTypeDetectors() {
        Object metricValueBool = false;

        Assert.assertTrue(Metric.Type.isBooleanMetric(metricValueBool));
        Assert.assertTrue(!Metric.Type.isNumericMetric(metricValueBool));
        Assert.assertTrue(!Metric.Type.isStringMetric(metricValueBool));

        Object metricValueNum = 1234567L;

        Assert.assertTrue(!Metric.Type.isBooleanMetric(metricValueNum));
        Assert.assertTrue(Metric.Type.isNumericMetric(metricValueNum));
        Assert.assertTrue(!Metric.Type.isStringMetric(metricValueNum));

        Object metricValueStr = "Foo";

        Assert.assertTrue(!Metric.Type.isBooleanMetric(metricValueStr));
        Assert.assertTrue(!Metric.Type.isNumericMetric(metricValueStr));
        Assert.assertTrue(Metric.Type.isStringMetric(metricValueStr));
    }
    
    @Test
    public void testGenericStatSet() {
        Average average = new Average(10, 30);
        AbstractRollupStat.set(average, 50);
        Assert.assertFalse(average.isFloatingPoint());
        
        // set as float (should get cast to double).
        AbstractRollupStat.set(average, 45f);
        // isFloatingPoint should have flipped.
        Assert.assertTrue(average.isFloatingPoint());
    }
}
