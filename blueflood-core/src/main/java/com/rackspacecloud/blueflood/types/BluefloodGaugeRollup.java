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

import java.io.IOException;
import java.util.Map;

public class BluefloodGaugeRollup extends BasicRollup {
    
    Points.Point<SimpleNumber> latestValue;

    public BluefloodGaugeRollup withLatest(long timestamp, Number value) {
        this.latestValue = new Points.Point<SimpleNumber>(timestamp, new SimpleNumber(value));
        this.setCount(Math.max(1, this.getCount()));
        return this;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BluefloodGaugeRollup))
            return false;
        else {
            BluefloodGaugeRollup other = (BluefloodGaugeRollup)obj;
            return super.equals(other) && other.latestValue.equals(this.latestValue);
        }
    }
    
    public long getTimestamp() {
        return latestValue.getTimestamp();
    }
    
    public Number getLatestNumericValue() {
        return latestValue.getData().getValue();
    }
    
    public SimpleNumber getLatestValue() {
        return latestValue.getData();
    }

    @Override
    public RollupType getRollupType() {
        return RollupType.GAUGE;
    }

    public static BluefloodGaugeRollup buildFromRawSamples(Points<SimpleNumber> input) throws IOException {
        
        // normal stuff.
        BluefloodGaugeRollup rollup = new BluefloodGaugeRollup();
        rollup.computeFromSimpleMetrics(input);
        
        // latest value is special.
        Points.Point<SimpleNumber> latest = null;
        for (Map.Entry<Long, Points.Point<SimpleNumber>> entry : input.getPoints().entrySet()) {
            if (latest == null || entry.getValue().getTimestamp() > latest.getTimestamp())
                latest = entry.getValue();
        }
        rollup.latestValue = latest;
        
        return rollup;
    }
    
    public static BluefloodGaugeRollup buildFromGaugeRollups(Points<BluefloodGaugeRollup> input) throws IOException {
        BluefloodGaugeRollup rollup = new BluefloodGaugeRollup();
        
        rollup.computeFromRollups(BasicRollup.recast(input, IBasicRollup.class));
        
        Points.Point<SimpleNumber> latest = rollup.latestValue;
        
        for (Map.Entry<Long, Points.Point<BluefloodGaugeRollup>> entry : input.getPoints().entrySet()) {
            if (latest == null || entry.getValue().getTimestamp() > latest.getTimestamp())
                latest = entry.getValue().getData().latestValue;
        }
        
        rollup.latestValue = latest;
        
        return rollup;
    }
    
    public static BluefloodGaugeRollup fromBasicRollup(IBasicRollup basic, long timestamp, Number latestValue) {
        BluefloodGaugeRollup rollup = new BluefloodGaugeRollup();
        
        rollup.setCount(basic.getCount());
        rollup.setAverage((Average)basic.getAverage());
        rollup.setMin((MinValue)basic.getMinValue());
        rollup.setMax((MaxValue)basic.getMaxValue());
        rollup.setVariance((Variance)basic.getVariance());
        
        rollup.latestValue = new Points.Point<SimpleNumber>(timestamp, new SimpleNumber(latestValue));
        
        return rollup;
    }
}
