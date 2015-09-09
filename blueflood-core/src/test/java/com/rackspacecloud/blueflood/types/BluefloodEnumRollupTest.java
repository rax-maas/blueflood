package com.rackspacecloud.blueflood.types;

import junit.framework.Assert;
import org.junit.Test;

/**
 * Created by tilo on 9/9/15.
 */
public class BluefloodEnumRollupTest {

    @Test
    public void testGetCountOfEnumRollup() {

        BluefloodEnumRollup rollup = new BluefloodEnumRollup().withHashedEnumValue(40L, 10L).withHashedEnumValue(50L,10L).withHashedEnumValue(70L,10L);
        Assert.assertTrue(rollup.getCount() == 3);

        BluefloodEnumRollup rollup1 = new BluefloodEnumRollup().withHashedEnumValue(100L, 10L).withHashedEnumValue(200L,10L).withHashedEnumValue(300L,10L);
        Assert.assertTrue(rollup1.getCount() == 3);
    }

    private static <T> Points<T> asPoints(Class<T> type, long initialTime, long timeDelta, T... values) {
        Points<T> points = new Points<T>();
        long time = initialTime;
        for (T v : values) {
            points.add(new Points.Point<T>(time, v));
            time += timeDelta;
        }
        return points;
    }
}
