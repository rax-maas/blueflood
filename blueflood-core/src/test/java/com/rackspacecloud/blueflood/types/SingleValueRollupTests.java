package com.rackspacecloud.blueflood.types;

import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class SingleValueRollupTests {
    private static final Random random = new Random(72262L);
    
    @Test
    public void testCounterRollupIdempotence() throws IOException {
        final CounterRollup cr0 = SingleValueRollupTests.buildCounterRollupFromLongs(makeRandomNumbers(1000));
        CounterRollup cumulative = CounterRollup.buildRollupFromCounterRollups(asPoints(CounterRollup.class, 0, 10, cr0));
        Assert.assertEquals(cr0, cumulative);
    }
    
    @Test
    public void testCounterRollupGeneration() throws IOException {
        final long[] src0 = makeRandomNumbers(1000);
        final long[] src1 = makeRandomNumbers(500);
        final CounterRollup cr0 = SingleValueRollupTests.buildCounterRollupFromLongs(src0);
        final CounterRollup cr1 = SingleValueRollupTests.buildCounterRollupFromLongs(src1);
        
        long expectedSum = sum(src0) + sum(src1);
        long expectedSamples = src0.length + src1.length;
        
        CounterRollup cumulative = CounterRollup.buildRollupFromCounterRollups(asPoints(CounterRollup.class, 0, 1000, cr0, cr1));
        
        Assert.assertEquals(expectedSum, cumulative.getCount());
        Assert.assertEquals(expectedSamples, cumulative.getNumSamplesUnsafe());
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
    
    private static long[] makeRandomNumbers(int count) {
        long[] numbers = new long[count];
        for (int i = 0; i < count; i++)
            numbers[i] = random.nextLong() % 1000000L;
        return numbers;
    }
    
    private static long sum(long[] numbers) {
        long sum = 0;
        for (long num : numbers)
            sum += num;
        return sum;
    }
    
    private static CounterRollup buildCounterRollupFromLongs(long... numbers) throws IOException {
        long count = 0;
        int numSamples = 0;
        for (long number : numbers) {
            count += number;
            numSamples += 1;
        }
        return new CounterRollup(numSamples).withCount(count);
    }
}
