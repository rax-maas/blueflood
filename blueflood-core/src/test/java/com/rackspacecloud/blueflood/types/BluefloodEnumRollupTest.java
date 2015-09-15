/*
 * Copyright 2015 Rackspace
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

import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class BluefloodEnumRollupTest {

    @Test
    public void testGetCountOfAllUniqueValuesEnumRollup() throws IOException{
        BluefloodEnumRollup rollup = new BluefloodEnumRollup().withHashedEnumValue(40L, 10L).withHashedEnumValue(50L,10L).withHashedEnumValue(70L,10L);
        Assert.assertTrue(rollup.getCount() == 3);

        BluefloodEnumRollup rollup1 = new BluefloodEnumRollup().withHashedEnumValue(100L, 10L).withHashedEnumValue(200L,10L).withHashedEnumValue(300L,10L);
        Assert.assertTrue(rollup1.getCount() == 3);

        BluefloodEnumRollup er = BluefloodEnumRollup.buildRollupFromEnumRollups(asPoints(BluefloodEnumRollup.class, 0, 300, rollup, rollup1));
        Assert.assertTrue(er.getCount() == 6);
    }

    @Test
    public void testGetCountOfDuplicateValuesEnumRollup() throws IOException{
        BluefloodEnumRollup rollup = new BluefloodEnumRollup().withHashedEnumValue(40L, 10L).withHashedEnumValue(40L,10L).withHashedEnumValue(40L,10L);
        Assert.assertTrue(rollup.getCount() == 1);

        BluefloodEnumRollup rollup1 = new BluefloodEnumRollup().withHashedEnumValue(40L, 10L).withHashedEnumValue(40L,10L).withHashedEnumValue(40L,10L);
        Assert.assertTrue(rollup1.getCount() == 1);

        BluefloodEnumRollup er = BluefloodEnumRollup.buildRollupFromEnumRollups(asPoints(BluefloodEnumRollup.class, 0, 300, rollup, rollup1));
        Assert.assertTrue(er.getCount() == 1);
    }

    @Test
    public void testMixOfUniqueAndDuplicateValuesEnumRollup() throws IOException {
        BluefloodEnumRollup rollup = new BluefloodEnumRollup().withHashedEnumValue(40L, 10L).withHashedEnumValue(40L,10L).withHashedEnumValue(40L,10L);
        Assert.assertTrue(rollup.getCount() == 1);

        BluefloodEnumRollup rollup1 = new BluefloodEnumRollup().withHashedEnumValue(30L, 10L).withHashedEnumValue(40L,10L).withHashedEnumValue(50L,10L);
        Assert.assertTrue(rollup1.getCount() == 3);

        BluefloodEnumRollup er = BluefloodEnumRollup.buildRollupFromEnumRollups(asPoints(BluefloodEnumRollup.class, 0, 300, rollup, rollup1));
        Assert.assertTrue(er.getCount() == 3);

        List<Long> lfound = new ArrayList<Long>(er.getHashedEnumValuesWithCounts().keySet());
        Collections.sort(lfound);

        List<Long> lexpected = new ArrayList<Long>(rollup1.getHashedEnumValuesWithCounts().keySet());
        Collections.sort(lexpected);

        Assert.assertEquals(lexpected, lfound);
        Map<Long, Long> map = er.getHashedEnumValuesWithCounts();
        for (Long hash : map.keySet()) {
            Long hashCount = map.get(hash);
            if (hash == 30L) {
                Assert.assertEquals(10L, (long)hashCount);
            }
            else if (hash == 40L) {
                Assert.assertEquals(40L, (long)hashCount);
            }
            else if (hash == 50L) {
                Assert.assertEquals(10L, (long)hashCount);
            }
        }
    }

    public static <T> Points<T> asPoints(Class<T> type, long initialTime, long timeDelta, T... values) {
        Points<T> points = new Points<T>();
        long time = initialTime;
        for (T v : values) {
            points.add(new Points.Point<T>(time, v));
            time += timeDelta;
        }
        return points;
    }
}
