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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class AverageTest {
    private static final double UNACCEPTABLE_DIFFERENCE = 0.000000001d;
    
    private static long[] LONG_SRC = new long[]{
        3,456,6,34,5,8,345,56,354,345,647,89,567,354,234,36,675,8,8,456,345,36,745,56,6786,795,687,456,345,346,
        456,332,435,346,34535665576L,4,346,4356,4,547,3456,345,37,568,3456,3426,3475,35,745,86,3456,346,3457,376,
        34,634,653,7,54687,4576,346,23,65,346347,457,45,75,67,3456,4356,345,73,445745,67457645,74,5754,6745,7457,
        457,3456,34,634,65,3456,347,4567,45,86756,865,7856,8745,66,345,634,5634,643,56,457,4567,54,7654,67,436534,56,346,
        34,53465,456,75467,4567,4576,45764357645L,673465,3456,3457,4567,45674567345634654L,756456,745674356345L,645367456L
    };
    
    @Test    
    public void testLongAverage() {
        Average avg = new Average();
        avg.add(2L);
        avg.add(4L);
        avg.add(4L);
        Assert.assertEquals(3L, avg.toLong());
    }
    
    @Test
    public void testDoubleAverage() {
        Average avg = new Average();
        avg.add(2.0D);
        avg.add(4.0D);
        avg.add(4.0D);
        Assert.assertEquals(3.3333333333333335D, avg.toDouble(), 0);
        
        // this a double, so average will be off due to rounding.
        Assert.assertEquals(3.3333333333333335D, avg.toDouble(), 0);
    }
   
    @Test
    public void testDoubleAveragingApproaches() {
        // standard average.
        double sum = 0;
        for (double d : TestData.DOUBLE_SRC) 
            sum += d;
        // avg = sum / src.length;
        
        // now compute using tugger. only possibility of overflow is in count.
        double average = 0;
        long count = 0;
        for (double v : TestData.DOUBLE_SRC)
            average += (v - average) / ++count;
        
        assert Math.abs(average - sum/TestData.DOUBLE_SRC.length) < 0.000001; // close enough?
        
    }
        
    @Test
    public void testLongAveragingApproaches() {
        // standard average.
        long sum = 0;
        for (long l : LONG_SRC)
            sum += l;
        // avg = sum / src.length;
       
        // now compute using mean+remainder method.
        // this approach is good to use if you're worried about overflow.  It's problem is that it is computationally
        // expensive.
        long mean = 0, remainder = 0;
        for (long v : LONG_SRC) {
            mean += v / LONG_SRC.length;
            remainder += v % LONG_SRC.length;
            mean += remainder / LONG_SRC.length;
            remainder %= LONG_SRC.length;
        }
        assert mean == sum/LONG_SRC.length;  
        
        // but what if we don't know src.length ahead of time?
        long rmean = 0, count = 0;
        remainder = 0;
        for (long v : LONG_SRC) {
            ++count;
            rmean += (v + remainder - rmean) / count;
            remainder = (v + remainder - rmean) % count;
        }
        Assert.assertTrue((double) Math.abs(rmean - mean) / (double) mean < UNACCEPTABLE_DIFFERENCE); // should be really close to the true mean.
    }
    
    @Test
    public void testFloatingRollup() {
        Average baseline = new Average();
        for (int i = 0; i < 1234; i++) 
            baseline.add(7d);
        for (int i = 0; i < 2565; i++)
            baseline.add(11d);
        for (int i = 0; i < 767; i++)
            baseline.add(17d);
        
        Average rollup = new Average();
        rollup.addBatch(7d, 1234);
        rollup.addBatch(11d, 2565);
        rollup.addBatch(17d, 767);
        
        Assert.assertTrue(Math.abs(rollup.toDouble() - baseline.toDouble()) < UNACCEPTABLE_DIFFERENCE);
    }
    
    @Test
    public void testLongRollup() {
        Average baseline = new Average();
        for (int i = 0; i < 1234; i++) 
            baseline.add(7L);
        for (int i = 0; i < 2565; i++)
            baseline.add(11L);
        for (int i = 0; i < 767; i++)
            baseline.add(17L);
        
        Average rollup = new Average();
        rollup.addBatch(7L, 1234);
        rollup.addBatch(11L, 2565);
        rollup.addBatch(17L, 767);
        
        Assert.assertEquals(baseline.toLong(), rollup.toLong());
    }

    @Test
    public void testAddRollup() throws IOException{
        Average avg = new Average(1, new Double(3.0));
        Points<SimpleNumber> data = new Points<SimpleNumber>();
        data.add(new Points.Point<SimpleNumber>(123456789L, new SimpleNumber(0.0)));
        data.add(new Points.Point<SimpleNumber>(123456770L, new SimpleNumber(0.0)));
        BasicRollup basicRollup = BasicRollup.buildRollupFromRawSamples(data);

        Assert.assertEquals(3.0, avg.toDouble(), 0);
        avg.handleRollupMetric(basicRollup);
        Assert.assertEquals(1.0, avg.toDouble(), 0);

        avg = new Average(1, new Long(3));
        Assert.assertEquals(3, avg.toLong());
        data =  new Points<SimpleNumber>();
        data.add(new Points.Point<SimpleNumber>(123456789L, new SimpleNumber(0)));
        data.add(new Points.Point<SimpleNumber>(123456770L, new SimpleNumber(0)));
        basicRollup = BasicRollup.buildRollupFromRawSamples(data);
        avg.handleRollupMetric(basicRollup);
        Assert.assertEquals(1, avg.toLong());
    }

    @Test
    public void testConstructorUnsupportedVariableType() {
       boolean failed = false;
       try {
           Average avg = new Average(1, new String("test"));
           Assert.fail();
       }
       catch (RuntimeException e) {
           Assert.assertEquals("Unexpected type: java.lang.String", e.getMessage());
           failed = true;
       }

       Assert.assertEquals(true, failed);
    }
}