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

import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class EnumMetricTest {
    final String TENANT_ID = "tenantid";
    final String METRIC_NAME = "metricname";

    @Test
    public void testCreateEnumMetric() {
        Locator locator = Locator.createLocatorFromPathComponents(TENANT_ID, METRIC_NAME);
        long metricValue = 12345;
        long collectionTime = System.currentTimeMillis();
        TimeValue ttl = new TimeValue(5, TimeUnit.HOURS);

        ArrayList<String> enumValues = new ArrayList<String>();
        enumValues.add("enum1");
        enumValues.add("enum2");
        enumValues.add("enum3");

        EnumMetric enumMetric = new EnumMetric(locator, metricValue, collectionTime, ttl, enumValues);
        Assert.assertEquals(TENANT_ID, enumMetric.getLocator().getTenantId());
        Assert.assertEquals(METRIC_NAME, enumMetric.getLocator().getMetricName());

        assert(enumMetric.getMetricValue().equals(metricValue));

        Assert.assertEquals(collectionTime, enumMetric.getCollectionTime());
        Assert.assertEquals(ttl.toSeconds(), enumMetric.getTtlInSeconds());
        Assert.assertEquals(DataType.NUMERIC, enumMetric.getDataType());

        Assert.assertNotNull(enumMetric.getEnumValues());
        assert(enumMetric.getEnumValues().equals(enumValues));

        Assert.assertEquals("Incorrect rollup type for EnumMetric", RollupType.BF_BASIC, enumMetric.getRollupType());
        Assert.assertEquals("tenantid.metricname:12345:N:18000:enum1,enum2,enum3", enumMetric.toString());
    }

    @Test
    public void testEquals() {
        Locator locator1 = Locator.createLocatorFromPathComponents(TENANT_ID, METRIC_NAME);
        Locator locator2 = Locator.createLocatorFromPathComponents(TENANT_ID, METRIC_NAME);

        long metricValue = 99999;
        long collectionTime = System.currentTimeMillis();
        TimeValue ttl = new TimeValue(5, TimeUnit.HOURS);

        ArrayList<String> enumValues = new ArrayList<String>();
        enumValues.add("e1");
        enumValues.add("e2");

        EnumMetric enumMetric1 = new EnumMetric(locator1, metricValue, collectionTime, ttl, enumValues);
        EnumMetric enumMetric2 = new EnumMetric(locator2, metricValue, collectionTime, ttl, enumValues);
        Assert.assertTrue("enumMetric1 should equal enumMetric2", enumMetric1.equals(enumMetric2));
        Assert.assertTrue("enumMetric2 should equal enumMetric1", enumMetric2.equals(enumMetric1));

        EnumMetric enumMetric3 = new EnumMetric(locator1, metricValue, collectionTime, ttl, null);
        EnumMetric enumMetric4 = new EnumMetric(locator2, metricValue, collectionTime, ttl, null);
        Assert.assertTrue("enumMetric3 should equal enumMetric4", enumMetric3.equals(enumMetric4));
        Assert.assertTrue("enumMetric4 should equal enumMetric3", enumMetric4.equals(enumMetric3));

        Assert.assertFalse("enumMetric1 should not equal to enumMetric3", enumMetric1.equals(enumMetric3));
        Assert.assertFalse("enumMetric3 should not equal to enumMetric1", enumMetric3.equals(enumMetric1));

        Assert.assertFalse("enumMetric2 should not equal to enumMetric4", enumMetric2.equals(enumMetric4));
        Assert.assertFalse("enumMetric4 should not equal to enumMetric2", enumMetric4.equals(enumMetric2));

        Assert.assertFalse("enumMetric1 should not be equal to non-instance of EnumMetric", enumMetric1.equals(new Object()));
    }

    @Test(expected=InvalidDataException.class)
    public void testInvalidCollectionTime() {
        new EnumMetric(null, null, -1, null, null);
    }

    @Test(expected=InvalidDataException.class)
    public void testInvalidTTL() {
        Locator locator = Locator.createLocatorFromPathComponents(TENANT_ID, METRIC_NAME);
        TimeValue invalidTTL = new TimeValue(-1, TimeUnit.SECONDS);
        new EnumMetric(locator, "", System.currentTimeMillis(), invalidTTL, null);
    }

    @Test
    public void testSetTtlInSeconds() {
        Locator locator = Locator.createLocatorFromPathComponents(TENANT_ID, METRIC_NAME);
        EnumMetric enumMetric = new EnumMetric(locator, "", 0, new TimeValue(1, TimeUnit.HOURS), null);
        int ttlInSeconds = 987654;
        enumMetric.setTtlInSeconds(ttlInSeconds);
        Assert.assertEquals(ttlInSeconds, enumMetric.getTtlInSeconds());
    }

    @Test(expected=InvalidDataException.class)
    public void testInvalidSetTTLInSeconds() {
        Locator locator = Locator.createLocatorFromPathComponents(TENANT_ID, METRIC_NAME);
        EnumMetric enumMetric = new EnumMetric(locator, "", 0, new TimeValue(1, TimeUnit.HOURS), null);
        int ttlInSeconds = -1;
        enumMetric.setTtlInSeconds(ttlInSeconds);
        Assert.assertEquals(ttlInSeconds, enumMetric.getTtlInSeconds());
    }
}
