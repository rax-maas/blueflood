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

import com.rackspacecloud.blueflood.utils.Util;

import java.io.IOException;

public class BluefloodCounterRollup implements Rollup {
    
    private Number count;
    private double rate; // per-second!

    /**
     * Number of pre-aggregated counters received by Blueflood
     */
    private int sampleCount;
    
    public BluefloodCounterRollup() {
        this.rate = 0d;
        this.sampleCount = 0;
    }
    
    public BluefloodCounterRollup withCount(Number count) {
        this.count = promoteToDoubleOrLong(count);
        return this;
    }
    
    public BluefloodCounterRollup withRate(double rate) {
        this.rate = rate;
        return this;
    }

    public BluefloodCounterRollup withSampleCount(int sampleCount) {
        this.sampleCount = sampleCount;
        return this;
    }
    
    public Number getCount() {
        return count;
    }
    
    public double getRate() {
        return rate;
    }

    public int getSampleCount() { return sampleCount; }
    
    private static Number promoteToDoubleOrLong(Number num) {
        if (num instanceof Float)
            return num.doubleValue();
        else if (num instanceof Integer)
            return num.longValue();
        return num;
    }

    @Override
    public String toString() {
        return String.format("count: %s, rate:%s", count.toString(), rate);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof BluefloodCounterRollup))
            return false;
        
        BluefloodCounterRollup other = (BluefloodCounterRollup)obj;
        return this.getCount().equals(other.getCount())
                && this.rate == other.rate
                && this.getSampleCount() == other.getSampleCount();
    }
    
    public static BluefloodCounterRollup buildRollupFromRawSamples(Points<SimpleNumber> input) throws IOException {
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        BluefloodCounterRollup rollup = new BluefloodCounterRollup();
        Number count = 0L;
        for (Points.Point<SimpleNumber> point : input.getPoints().values()) {
            count = sum(count, point.getData().getValue());
            minTime = Math.min(minTime, point.getTimestamp());
            maxTime = Math.max(maxTime, point.getTimestamp());
        }
        double numSeconds = (double)(maxTime - minTime);
        double rate = count.doubleValue() / numSeconds;
        return rollup.withCount(count).withRate(rate).withSampleCount(input.getPoints().size());
    }

    public static BluefloodCounterRollup buildRollupFromCounterRollups(Points<BluefloodCounterRollup> input) throws IOException {
        
        Number count = 0L;
        double seconds = 0;
        int sampleCount = 0;
        for (Points.Point<BluefloodCounterRollup> point : input.getPoints().values()) {
            count = sum(count, point.getData().getCount());
            sampleCount = sampleCount + point.getData().getSampleCount();
            seconds += Util.safeDiv(point.getData().getCount().doubleValue(), point.getData().getRate());
        }
        double aggregateRate = Util.safeDiv(count.doubleValue(), seconds);

        return new BluefloodCounterRollup().withCount(count).withRate(aggregateRate).withSampleCount(sampleCount);
    }
    
    private static Number sum(Number x, Number y) {
        boolean isDouble = x instanceof Double || x instanceof Float || y instanceof Double || y instanceof Float;
        if (isDouble)
            return x.doubleValue() + y.doubleValue();
        else
            return x.longValue() + y.longValue();
    }

    @Override
    public Boolean hasData() {
        return sampleCount > 0;
    }

    @Override
    public RollupType getRollupType() {
        return RollupType.COUNTER;
    }
}
