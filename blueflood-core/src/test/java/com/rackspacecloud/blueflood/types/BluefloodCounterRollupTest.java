package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.io.Constants;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class BluefloodCounterRollupTest {
    private static final Random random = new Random(72262L);
    
    @Test
    public void testRateCalculations() throws IOException {
        long[] sample0 = {10L,10L,10L,10L,10L,10L,10L,10L,10L,10L}; // 300 secs
        long[] sample1 = {20L,20L,20L,20L,20L,20L,20L,20L,20L,20L,20L,20L,20L,20L,20L}; // 450 secs
        final BluefloodCounterRollup cr0 = BluefloodCounterRollupTest.buildCounterRollupFromLongs(sample0);
        final BluefloodCounterRollup cr1 = BluefloodCounterRollupTest.buildCounterRollupFromLongs(sample1);
        
        Assert.assertEquals(100d/300d, cr0.getRate());
        Assert.assertEquals(300d/450d, cr1.getRate());
        
        // great, now combine them.
        BluefloodCounterRollup cr2 = BluefloodCounterRollup.buildRollupFromCounterRollups(asPoints(BluefloodCounterRollup.class, 0, 300, cr0, cr1));
        
        Assert.assertEquals(400d/750d, cr2.getRate());
    }
    
    @Test
    public void testCounterRollupIdempotence() throws IOException {
        final BluefloodCounterRollup cr0 = BluefloodCounterRollupTest.buildCounterRollupFromLongs(makeRandomNumbers(1000));
        BluefloodCounterRollup cumulative = BluefloodCounterRollup.buildRollupFromCounterRollups(asPoints(BluefloodCounterRollup.class, 0, 10, cr0));
        Assert.assertEquals(cr0, cumulative);
    }
    
    @Test
    public void testCounterRollupGeneration() throws IOException {
        final long[] src0 = makeRandomNumbers(1000);
        final long[] src1 = makeRandomNumbers(500);
        final BluefloodCounterRollup cr0 = BluefloodCounterRollupTest.buildCounterRollupFromLongs(src0);
        final BluefloodCounterRollup cr1 = BluefloodCounterRollupTest.buildCounterRollupFromLongs(src1);
        
        long expectedSum = sum(src0) + sum(src1);
        
        BluefloodCounterRollup cumulative = BluefloodCounterRollup.buildRollupFromCounterRollups(asPoints(BluefloodCounterRollup.class, 0, 1000, cr0, cr1));
        
        Assert.assertEquals(expectedSum, cumulative.getCount());
    }

    @Test
    public void testNullCounterRollupVersusZero() throws IOException {
        final long[] data = new long[]{0L, 0L, 0L};
        final long[] no_data = new long[]{};
        final BluefloodCounterRollup crData = BluefloodCounterRollupTest.buildCounterRollupFromLongs(data);
        final BluefloodCounterRollup crNoData = BluefloodCounterRollupTest.buildCounterRollupFromLongs(no_data);
        Assert.assertNotSame(crData, crNoData);
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
    
    // assume the samples are 30s apart.
    private static BluefloodCounterRollup buildCounterRollupFromLongs(long... numbers) throws IOException {
        long count = 0;
        for (long number : numbers) {
            count += number;
        }
        long sum = sum(numbers);
        double rate = (double)sum / (double)(Constants.DEFAULT_SAMPLE_INTERVAL * numbers.length);
        return new BluefloodCounterRollup().withCount(count).withRate(rate).withSampleCount(numbers.length);
    }
}
