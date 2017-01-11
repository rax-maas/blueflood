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
    public void testTTL() {
        Locator locator = Locator.createLocatorFromPathComponents("tenantId", "metricName");
        Metric metric = new Metric(locator, 134891734L, System.currentTimeMillis(), new TimeValue(5, TimeUnit.HOURS), "Unknown");

        try {
            metric.setTtl(new TimeValue(Long.MAX_VALUE, TimeUnit.SECONDS));
            fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RuntimeException);
        }
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
