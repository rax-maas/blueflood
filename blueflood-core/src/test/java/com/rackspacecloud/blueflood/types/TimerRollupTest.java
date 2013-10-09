package com.rackspacecloud.blueflood.types;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

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
    
    private static void assertRollupsAreClose(IBasicRollup expected, IBasicRollup actual) {
        Assert.assertEquals("average", expected.getAverage(), actual.getAverage());
        Assert.assertEquals("count", expected.getCount(), actual.getCount());
        Assert.assertEquals("min", expected.getMinValue(), actual.getMinValue());
        Assert.assertEquals("max", expected.getMaxValue(), actual.getMaxValue());
        Assert.assertEquals("variance", expected.getVariance(), actual.getVariance());
    }
    
    @Test
    public void testConstantTimerRollup() throws IOException {
        // 40 samples, one per milli, each sample increments by one.  Now divide those into four parts.
        TimerRollup tr0 = new TimerRollup().withSum(45).withCountPS(4.5d).withAverage(4).withVariance(8.25d).withMinValue(0).withMaxValue(9).withCount(10);
        TimerRollup tr1 = new TimerRollup().withSum(145).withCountPS(4.5d).withAverage(14).withVariance(8.25d).withMinValue(10).withMaxValue(19).withCount(10);
        TimerRollup tr2 = new TimerRollup().withSum(245).withCountPS(4.5d).withAverage(24).withVariance(8.25d).withMinValue(20).withMaxValue(29).withCount(10);
        TimerRollup tr3 = new TimerRollup().withSum(345).withCountPS(4.5d).withAverage(34).withVariance(8.25d).withMinValue(30).withMaxValue(39).withCount(10);
        BasicRollup br0 = BasicRollup.buildRollupFromRawSamples(TimerRollupTest.createPoints(0, 0, 10, 1, 1));
        BasicRollup br1 = BasicRollup.buildRollupFromRawSamples(TimerRollupTest.createPoints(10, 10, 10, 1, 1));
        BasicRollup br2 = BasicRollup.buildRollupFromRawSamples(TimerRollupTest.createPoints(20, 20, 10, 1, 1));
        BasicRollup br3 = BasicRollup.buildRollupFromRawSamples(TimerRollupTest.createPoints(30, 30, 10, 1, 1));
        
        // first, make sure that the self-proclaimed timers match the basic rollups generated from raw samples.
        // this establishes a baseline.
        assertRollupsAreClose(br0, tr0);
        assertRollupsAreClose(br1, tr1);
        assertRollupsAreClose(br2, tr2);
        assertRollupsAreClose(br3, tr3);
        
        // create a cumulative timer from the timer rollups.
        Points<TimerRollup> timerPoints = new Points<TimerRollup>();
        timerPoints.add(new Points.Point<TimerRollup>(0, tr0));
        timerPoints.add(new Points.Point<TimerRollup>(10, tr1));
        timerPoints.add(new Points.Point<TimerRollup>(20, tr2));
        timerPoints.add(new Points.Point<TimerRollup>(30, tr3));
        TimerRollup cumulativeTimer = TimerRollup.buildRollupFromTimerRollups(timerPoints);
        
        // create a cumulative basic from the basic rollups
        Points<BasicRollup> rollupPoints = new Points<BasicRollup>();
        rollupPoints.add(new Points.Point<BasicRollup>(0, br0));
        rollupPoints.add(new Points.Point<BasicRollup>(10, br1));
        rollupPoints.add(new Points.Point<BasicRollup>(20, br2));
        rollupPoints.add(new Points.Point<BasicRollup>(30, br3));
        BasicRollup cumulativeBasicFromRollups = BasicRollup.buildRollupFromRollups(rollupPoints);
        
        // also create a cumulative basic from raw data.
        Points<SimpleNumber> simplePoints = TimerRollupTest.createPoints(0, 0, 40, 1, 1);
        BasicRollup cumulativeBasicFromRaw = BasicRollup.buildRollupFromRawSamples(simplePoints);
        
        // ensure the baseline holds: cumulative basic from raw should be the same as cumulative basic from basic.
        assertRollupsAreClose(cumulativeBasicFromRaw, cumulativeBasicFromRollups);
        
        // now the real test: cumulative timer from timers should be the same as cumulative basic from {basic | raw}
        assertRollupsAreClose(cumulativeBasicFromRaw, cumulativeTimer);
        assertRollupsAreClose(cumulativeBasicFromRollups, cumulativeTimer);
        
        Assert.assertEquals(4.5d, cumulativeTimer.getCountPS());
    }
    
    @Test
    public void testVariableRateTimerRollup() throws IOException {
        // 200 time units worth of data gathered from 100 samples.
        Points<SimpleNumber> p0 = createPoints(0, 0, 100, 2, 2);
        // count_ps for this will be 200 time units / 100 samples = 2.0
        BasicRollup br0 = BasicRollup.buildRollupFromRawSamples(p0);
        final TimerRollup tr0 = new TimerRollup()
                .withSum(9900)
                .withCountPS(2.0)
                .withAverage(99)
                .withVariance(3333.0d)
                .withMinValue(0)
                .withMaxValue(198)
                .withCount(100);
        
        // 100 time units worth of data gathered from 200 samples.
        Points<SimpleNumber> p1 = createPoints(200, 200, 200, 1, 2);
        // count_ps for this will be 200 time units / 200 samples = 1.0
        BasicRollup br1 = BasicRollup.buildRollupFromRawSamples(p1);
        final TimerRollup tr1 = new TimerRollup()
                .withSum(39900)
                .withCountPS(1.0)
                .withAverage(399)
                .withVariance(13333.0d)
                .withMinValue(200)
                .withMaxValue(598)
                .withCount(100);
        
        // count_ps should end up being 400 time units / 300 samples = 1.33
        TimerRollup cumulative = TimerRollup.buildRollupFromTimerRollups(new Points<TimerRollup>() {{
            add(new Point<TimerRollup>(0, tr0));
            add(new Point<TimerRollup>(200, tr1));
        }});
        
        Assert.assertEquals(4d/3d, cumulative.getCountPS());
    }
    
    private static final Collection<Number> longs = new ArrayList<Number>() {{
        add(1L); add(2L); add(3L); // sum=6L
    }};
    private static final Collection<Number> doubles = new ArrayList<Number>() {{
        add(1.0d); add(2.0d); add(3.0d); // sum=6.0d
    }};
    private static Collection<Number> mixed = new ArrayList<Number>() {{
        add(1L); add(2.0d); add(3L); // sum=6.0d
    }};
    Collection<Number> alsoMixed = new ArrayList<Number>() {{
        add(1.0d); add(2L); add(3.0d); // 6.0d
    }};
    
    @Test
    public void testSum() {
        Assert.assertEquals(6L, TimerRollup.sum(longs));
        Assert.assertEquals(6.0d, TimerRollup.sum(doubles));
        Assert.assertEquals(6.0d, TimerRollup.sum(mixed));
        Assert.assertEquals(6.0d, TimerRollup.sum(alsoMixed));
    }
    
    @Test
    public void testAverage() {
        Assert.assertEquals(2L, TimerRollup.avg(longs));
        Assert.assertEquals(2.0d, TimerRollup.avg(doubles));
        Assert.assertEquals(2.0d, TimerRollup.avg(mixed));
        Assert.assertEquals(2.0d, TimerRollup.avg(alsoMixed));
    }
    
    @Test
    public void testMax() {
        Assert.assertEquals(3L, TimerRollup.max(longs));
        Assert.assertEquals(3.0d, TimerRollup.max(doubles));
        Assert.assertEquals(3.0d, TimerRollup.max(mixed));
        Assert.assertEquals(3.0d, TimerRollup.max(alsoMixed));
    }
    
    @Test
    public void testPercentiles() throws IOException {
        final TimerRollup tr0 = new TimerRollup().withSum(0).withCountPS(0).withAverage(0).withVariance(0).withMinValue(0).withMaxValue(0).withCount(0);
        final TimerRollup tr1 = new TimerRollup().withSum(0).withCountPS(0).withAverage(0).withVariance(0).withMinValue(0).withMaxValue(0).withCount(0);
        
        // populate percentiles (these are nonsensical)
        tr0.setPercentile("75", 0.1d, 100, 0.2d);
        tr1.setPercentile("75", 0.2d, 200, 0.3d);
        // todo: do we want weighted means? e.g.: ((0.1d * 100d) + (0.2d * 200d)) / (100d + 200d)
        double expectedMean75 = (0.1d + 0.2d) / 2.0d;
        
        tr0.setPercentile("98", 0.3d, 300, 0.4d);
        tr1.setPercentile("98", 0.4d, 400, 0.5d);
        // weighted would be: ((0.3d * 300d) + (0.4d * 400d)) / (300d + 400d);
        double expectedMean98 = (0.3d + 0.4d) / 2.0d;
        
        TimerRollup cumulative = TimerRollup.buildRollupFromTimerRollups(new Points<TimerRollup>() {{
            add(new Point<TimerRollup>(0, tr0));
            add(new Point<TimerRollup>(100, tr1));
        }});

        Assert.assertEquals(2, cumulative.getPercentiles().size());
        Assert.assertTrue(Math.abs(expectedMean75 - cumulative.getPercentiles().get("75").getMean().doubleValue()) < ACCEPTABLE_SKEW);
        Assert.assertTrue(Math.abs(expectedMean98 - cumulative.getPercentiles().get("98").getMean().doubleValue()) < ACCEPTABLE_SKEW);
    }
    
    @Test
    public void tesLinedListMultimapAllowsDuplicates() {
        // NOTE: HashMultimap behaves differently. duplicates are not allowed.
        Multimap<String, Number> lmap = LinkedListMultimap.create();
        lmap.put("foo", 1);
        lmap.put("foo", 2);
        lmap.put("foo", 1);
        Assert.assertEquals(3, lmap.size());
        Assert.assertEquals(3, lmap.get("foo").size());
    }
    
}
