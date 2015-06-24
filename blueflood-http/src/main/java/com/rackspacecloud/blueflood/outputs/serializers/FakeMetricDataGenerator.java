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

package com.rackspacecloud.blueflood.outputs.serializers;

import com.bigml.histogram.Bin;
import com.bigml.histogram.SimpleTarget;
import com.bigml.histogram.Target;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.CounterRollup;
import com.rackspacecloud.blueflood.types.GaugeRollup;
import com.rackspacecloud.blueflood.types.HistogramRollup;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.SetRollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import com.rackspacecloud.blueflood.types.TimerRollup;

import java.util.ArrayList;
import java.util.Collection;

public class FakeMetricDataGenerator {
    public static Points<SimpleNumber> generateFakeFullResPoints() {
        Points<SimpleNumber> points = new Points<SimpleNumber>();

        long baseTime = 1234567L;
        for (int count = 0; count < 5; count++) {
            Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(baseTime + (count*1000), new SimpleNumber((long) count));
            points.add(point);
        }

        return points;
    }

    public static Points<BasicRollup> generateFakeRollupPoints() {
        Points<BasicRollup> points = new Points<BasicRollup>();

        long baseTime = 1234567L;
        for (int count = 0; count < 5; count++) {
            final BasicRollup basicRollup = new BasicRollup();
            basicRollup.setCount(count * 100);
            basicRollup.getAverage().setLongValue(count);
            Points.Point<BasicRollup> point = new Points.Point<BasicRollup>(baseTime + (count*1000), basicRollup);
            points.add(point);
        }

        return points;
    }

    public static Points<String> generateFakeStringPoints() {
        Points<String> points = new Points<String>();
        long startTime = 1234567L;
        for (int i =0; i < 5; i++) {
            long timeNow = startTime + i*1000;
            Points.Point<String> point = new Points.Point<String>(timeNow, String.valueOf(timeNow));
            points.add(point);
        }
        return points;
    }

    public static Points<HistogramRollup> generateFakeHistogramRollupPoints() {
        Points<HistogramRollup> points = new Points<HistogramRollup>();
        long startTime = 1234567L;
        for (int i =0; i < 5; i++) {
            long timeNow = startTime + i*1000;
            Points.Point<HistogramRollup> point = new Points.Point<HistogramRollup>(timeNow,
                    new HistogramRollup(getBins()));
            points.add(point);
        }
        return points;
    }

    private static Collection<Bin<SimpleTarget>> getBins() {
        Collection<Bin<SimpleTarget>> bins = new ArrayList<Bin<SimpleTarget>>();
        for (int i = 1; i < 3; i++) {
            bins.add(new Bin(55.55 + i, (double) i, SimpleTarget.TARGET));
        }
        return bins;
    }
    
    public static Points<CounterRollup> generateFakeCounterRollupPoints() {
        Points<CounterRollup> points = new Points<CounterRollup>();
        long startTime = 1234567L;
        for (int i = 0; i < 5; i++) {
            long timeNow = startTime + i*1000;
            Points.Point<CounterRollup> point = new Points.Point<CounterRollup>(timeNow, new CounterRollup()
                    .withCount(i + 1000)
                    .withRate((double) i)
                    .withSampleCount(1));
            points.add(point);
        }
        return points;
    }
    
    public static Points<SetRollup> generateFakeSetRollupPoints() {
        Points<SetRollup> points = new Points<SetRollup>();
        long startTime = 1234567L;
        for (int i = 0; i < 5; i++) {
            long timeNow = startTime + i*1000;
            Points.Point<SetRollup> point = new Points.Point<SetRollup>(timeNow, new SetRollup()
                    .withObject(i)
                    .withObject(i % 2)
                    .withObject(i / 2));
            points.add(point);
        }
        return points;
    }
    
    public static Points<GaugeRollup> generateFakeGaugeRollups() {
        Points<GaugeRollup> points = new Points<GaugeRollup>();
        long startTime = 1234567L;
        for (int i = 0; i < 5; i++) {
            long timeNow = startTime + i*1000;
            Points.Point<GaugeRollup> point = new Points.Point<GaugeRollup>(timeNow, new GaugeRollup()
                .withLatest(timeNow, i));
            points.add(point);
        }
        return points;
    }
    
    public static Points<TimerRollup> generateFakeTimerRollups() {
        Points<TimerRollup> points = new Points<TimerRollup>();
        long startTime = 1234567L;
        for (int i = 0; i < 5; i++) {
            long timeNow = startTime + i*1000;
            TimerRollup rollup = new TimerRollup()
                .withAverage(i)
                .withCount(i)
                .withCountPS(i*0.1d)
                .withMaxValue(i)
                .withMinValue(i)
                .withSum(Double.valueOf(i+i))
                .withVariance(i);
            rollup.setPercentile("50", i);
            rollup.setPercentile("99", i * 2 + 1);
            Points.Point<TimerRollup> point = new Points.Point<TimerRollup>(timeNow, rollup);
            points.add(point);
        }
        return points;
    }
}
