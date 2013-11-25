package com.rackspacecloud.blueflood.types;

import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class CounterRollupTest {
    private static final Random random = new Random(72262L);
    
    @Test
    public void testGenerationFromRaw() throws IOException {
        long[] src = makeRandomNumbers(1000);
        Points<SimpleNumber> points = makeNumericPoints(src, 0);
        
        long expectedSum = sum(src);
        CounterRollup rollup = CounterRollup.buildRollupFromRawSamples(points);

        Assert.assertEquals(src.length, rollup.getNumSamples());
        Assert.assertEquals(expectedSum, rollup.getCount());
    }
    
    @Test
    public void testGenerationFromRollups() throws IOException {
        final long[] src0 = makeRandomNumbers(1000);
        final long[] src1 = makeRandomNumbers(500);
        final CounterRollup cr0 = CounterRollup.buildRollupFromRawSamples(makeNumericPoints(src0, 0));
        final CounterRollup cr1 = CounterRollup.buildRollupFromRawSamples(makeNumericPoints(src1, src0.length));
        
        long expectedSum = sum(src0) + sum(src1);
        long expectedSamples = src0.length + src1.length;
        
        CounterRollup cumulative = CounterRollup.buildRollupFromCounterRollups(new Points<CounterRollup>() {{
            add(new Point<CounterRollup>(0, cr0));
            add(new Point<CounterRollup>(src0.length, cr1));
        }});
        
        Assert.assertEquals(expectedSum, cumulative.getCount());
        Assert.assertEquals(expectedSamples, cumulative.getNumSamples());
    }
    
    private static long[] makeRandomNumbers(int count) {
        long[] numbers = new long[count];
        for (int i = 0; i < count; i++)
            numbers[i] = random.nextLong() % 1000000L;
        return numbers;
    }
    
    private static Points<SimpleNumber> makeNumericPoints(long[] src, long ts) {
        Points<SimpleNumber> points =new Points<SimpleNumber>();
        for (long num : src)
            points.add(new Points.Point<SimpleNumber>(ts++, new SimpleNumber(num)));
        return points;
    }
    
    private static long sum(long[] numbers) {
        long sum = 0;
        for (long num : numbers)
            sum += num;
        return sum;
    }
}
