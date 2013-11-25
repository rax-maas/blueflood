package com.rackspacecloud.blueflood.types;

import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;

public class TimerRollupTest {
    
    private static final double ACCEPTABLE_SKEW = 0.00000001d;
    
    @Test
    public void testCountPerSecondCalculation() {
        Assert.assertEquals(7.5d, TimerRollup.calculatePerSecond(150, 5d, 300, 10d));
    }
    
    private static Points<SimpleNumber> createPoints(final long startingTime, final int startingValue, final int numSamples, final int timeDelta, final int sampleDelta) {
        return new Points<SimpleNumber>() {{
            for (int i = 0; i < numSamples; i++)
                add(new Point<SimpleNumber>(startingTime + (i *timeDelta), new SimpleNumber(startingValue + (i * sampleDelta))));
        }};
    } 
    
    @Test
    public void testConstantTimerRollup() throws IOException {
        // 40 samples, one per milli, each sample increments by one.  Now divide those into four parts.
        TimerRollup tr0 = new TimerRollup(45, 4.5d, 4, 8.25d, 0, 9, 10);
        TimerRollup tr1 = new TimerRollup(145, 4.5d, 14, 8.25d, 10, 19, 10);
        TimerRollup tr2 = new TimerRollup(245, 4.5d, 24, 8.25d, 20, 29, 10);
        TimerRollup tr3 = new TimerRollup(345, 4.5d, 34, 8.25d, 30, 39, 10);
        Points<TimerRollup> timerPoints = new Points<TimerRollup>();
        timerPoints.add(new Points.Point<TimerRollup>(0, tr0));
        timerPoints.add(new Points.Point<TimerRollup>(10, tr1));
        timerPoints.add(new Points.Point<TimerRollup>(20, tr2));
        timerPoints.add(new Points.Point<TimerRollup>(30, tr3));
        TimerRollup cumulativeTimer = TimerRollup.buildRollupFromTimerRollups(timerPoints);
        
        Points<SimpleNumber> simplePoints = TimerRollupTest.createPoints(0, 0, 40, 1, 1);
        BasicRollup cumulativeBasic = BasicRollup.buildRollupFromRawSamples(simplePoints);
        
        Assert.assertEquals(cumulativeBasic.getAverage(), cumulativeTimer.getAverage());
        Assert.assertEquals(cumulativeBasic.getCount(), cumulativeTimer.getCount());
        Assert.assertEquals(cumulativeBasic.getMinValue(), cumulativeTimer.getMinValue());
        Assert.assertEquals(cumulativeBasic.getMaxValue(), cumulativeTimer.getMaxValue());
        Assert.assertEquals(cumulativeBasic.getVariance(), cumulativeTimer.getVariance());
        
        Assert.assertEquals(4.5d, cumulativeTimer.getCountPS());
    }
    
    @Test
    public void testVariableRateTimerRollup() throws IOException {
        // 200 time units worth of data gathered from 100 samples.
        Points<SimpleNumber> p0 = createPoints(0, 0, 100, 2, 2);
        // count_ps for this will be 200 time units / 100 samples = 2.0
        BasicRollup br0 = BasicRollup.buildRollupFromRawSamples(p0);
        final TimerRollup tr0 = new TimerRollup(9900, 2.0, 99, 3333.0d, 0, 198, 100);
        
        // 100 time units worth of data gathered from 200 samples.
        Points<SimpleNumber> p1 = createPoints(200, 200, 200, 1, 2);
        // count_ps for this will be 200 time units / 200 samples = 1.0
        BasicRollup br1 = BasicRollup.buildRollupFromRawSamples(p1);
        final TimerRollup tr1 = new TimerRollup(39900, 1.0, 399, 13333.0d, 200, 598, 100);
        
        // count_ps should end up being 400 time units / 300 samples = 1.33
        TimerRollup cumulative = TimerRollup.buildRollupFromTimerRollups(new Points<TimerRollup>() {{
            add(new Point<TimerRollup>(0, tr0));
            add(new Point<TimerRollup>(200, tr1));
        }});
        
        Assert.assertEquals(4d/3d, cumulative.getCountPS());
    }
    
    @Test
    public void testPercentiles() throws IOException {
        final TimerRollup tr0 = new TimerRollup(0, 0, 0, 0, 0, 0, 0);
        final TimerRollup tr1 = new TimerRollup(0, 0, 0, 0, 0, 0, 0);
        
        // populate percentiles (these are nonsensical)
        tr0.setPercentile("75", 0.1d, 100, 0.2d, 0.3d);
        tr1.setPercentile("75", 0.2d, 200, 0.3d, 0.4d);
        double expectedMean75 = ((0.1d * 100d) + (0.2d * 200d)) / (100d + 200d);
        
        tr0.setPercentile("98", 0.3d, 300, 0.4d, 0.5d);
        tr1.setPercentile("98", 0.4d, 400, 0.5d, 0.6d);
        double expectedMean98 = ((0.3d * 300d) + (0.4d * 400d)) / (300d + 400d);
        
        TimerRollup cumulative = TimerRollup.buildRollupFromTimerRollups(new Points<TimerRollup>() {{
            add(new Point<TimerRollup>(0, tr0));
            add(new Point<TimerRollup>(100, tr1));
        }});

        Assert.assertEquals(2, cumulative.getPercentiles().size());
        Assert.assertTrue(Math.abs(expectedMean75 - cumulative.getPercentiles().get("75").getAverage().toDouble()) < ACCEPTABLE_SKEW);
        Assert.assertTrue(Math.abs(expectedMean98 - cumulative.getPercentiles().get("98").getAverage().toDouble()) < ACCEPTABLE_SKEW);
    }
    
}
