/*
 * Copyright 2014 Rackspace
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

package com.rackspacecloud.blueflood.inputs.handlers;

import com.google.gson.internal.LazilyParsedNumber;
import com.rackspacecloud.blueflood.inputs.handlers.wrappers.AggregatedPayload;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PreaggregateConversions {
    
    // todo: punt on TTL
    private static final TimeValue DEFAULT_TTL = new TimeValue(48, TimeUnit.HOURS);
    private static final String NAME_DELIMITER = "//.";
    
    // NOTE: when you create objects from gson-converted json, you need to make sure to resolve numbers that
    // are not accessed via `doubleValue()` or `longValue()`, i.e., they are treated as `Number` instances.
    // the Number supplied by gson is and instance of LazilyParsedNumber and will cause breakage in certain
    // circumstances. e.g. calling `longValue()` when the number is obviously a double, or is used in the
    // type comparisions we use to determine how to serialize a number.
    
    public static Collection<IMetric> buildMetricsCollection(AggregatedPayload payload) {
        Collection<IMetric> metrics = new ArrayList<IMetric>();
        metrics.addAll(PreaggregateConversions.convertCounters(payload.getTenantId(), payload.getTimestamp(), payload.getFlushIntervalMillis(), payload.getCounters()));
        metrics.addAll(PreaggregateConversions.convertGauges(payload.getTenantId(), payload.getTimestamp(), payload.getGauges()));
        metrics.addAll(PreaggregateConversions.convertSets(payload.getTenantId(), payload.getTimestamp(), payload.getSets()));
        metrics.addAll(PreaggregateConversions.convertTimers(payload.getTenantId(), payload.getTimestamp(), payload.getTimers()));
        return metrics;
    }

    public static Collection<PreaggregatedMetric> convertCounters(String tenant, long timestamp, long flushIntervalMillis, Collection<BluefloodCounter> counters) {
        List<PreaggregatedMetric> list = new ArrayList<PreaggregatedMetric>(counters.size());
        for (BluefloodCounter counter : counters) {
            Locator locator = Locator.createLocatorFromPathComponents(tenant, counter.getName().split(NAME_DELIMITER, -1));
            // flushIntervalMillis could be zero (if not specified in the statsD config).
            long sampleCount = flushIntervalMillis > 0
                    ? (long)(counter.getRate().doubleValue() * ((double)flushIntervalMillis/1000d))
                    : 1;
            Rollup rollup = new BluefloodCounterRollup()
                    .withCount(resolveNumber(counter.getValue()))
                    .withRate(counter.getRate().doubleValue())
                    .withSampleCount((int)sampleCount);
            PreaggregatedMetric metric = new PreaggregatedMetric(timestamp, locator, DEFAULT_TTL, rollup);
            list.add(metric);
        }
        return list;
    }
    
    public static Collection<PreaggregatedMetric> convertGauges(String tenant, long timestamp, Collection<BluefloodGauge> gauges) {
        List<PreaggregatedMetric> list = new ArrayList<PreaggregatedMetric>(gauges.size());
        for (BluefloodGauge gauge : gauges) {
            Locator locator = Locator.createLocatorFromPathComponents(tenant, gauge.getName().split(NAME_DELIMITER, -1));
            Points<SimpleNumber> points = new Points<SimpleNumber>();
            points.add(new Points.Point<SimpleNumber>(timestamp, new SimpleNumber(resolveNumber(gauge.getValue()))));
            try {
                Rollup rollup = BluefloodGaugeRollup.buildFromRawSamples(points);
                PreaggregatedMetric metric = new PreaggregatedMetric(timestamp, locator, DEFAULT_TTL, rollup);
                list.add(metric);
            } catch (IOException ex) {
                throw new IOError(ex);
            }   
        }
        return list;
    }
    
    public static Collection<PreaggregatedMetric> convertTimers(String tenant, long timestamp, Collection<BluefloodTimer> timers) {
        List<PreaggregatedMetric> list = new ArrayList<PreaggregatedMetric>(timers.size());
        for (BluefloodTimer timer : timers) {
            Locator locator = Locator.createLocatorFromPathComponents(tenant, timer.getName().split(NAME_DELIMITER, -1));
            BluefloodTimerRollup rollup = new BluefloodTimerRollup()
                    .withCount(timer.getCount().longValue())
                    .withSampleCount(1)
                    .withAverage(resolveNumber(timer.getAvg() == null ? 0.0d : timer.getAvg()))
                    .withMaxValue(resolveNumber(timer.getMax() == null ? 0.0d : timer.getMax()))
                    .withMinValue(resolveNumber(timer.getMin() == null ? 0.0d : timer.getMin()))
                    .withCountPS(timer.getRate() == null ? 0.0d : timer.getRate().doubleValue())
                    .withSum(timer.getSum() == null ? 0L : timer.getSum().doubleValue())
                    .withVariance(Math.pow(timer.getStd() == null ? 0.0d : timer.getStd().doubleValue(), 2d));
            for (Map.Entry<String, Percentile> entry : timer.getPercentiles().entrySet()) {
                // throw away max and sum.
                if (entry.getValue().getAvg() != null) {
                    rollup.setPercentile(entry.getKey(), resolveNumber(entry.getValue().getAvg()));
                }
            }
            PreaggregatedMetric metric = new PreaggregatedMetric(timestamp, locator, DEFAULT_TTL, rollup);
            list.add(metric);
        }
        return list;
    }
    
    public static Collection<PreaggregatedMetric> convertSets(String tenant, long timestamp, Collection<BluefloodSet> sets) {
        List<PreaggregatedMetric> list = new ArrayList<PreaggregatedMetric>(sets.size());
        for (BluefloodSet set : sets) {
            Locator locator = Locator.createLocatorFromPathComponents(tenant, set.getName().split(NAME_DELIMITER, -1));
            BluefloodSetRollup rollup = new BluefloodSetRollup();
            for (String value : set.getValues()) {
                rollup = rollup.withObject(value);
            }
            PreaggregatedMetric metric = new PreaggregatedMetric(timestamp, locator, DEFAULT_TTL, rollup);
            list.add(metric);
        }
        return list;
    }
    
    // resolve a number to a Long or double.
    public static Number resolveNumber(Number n) {
        if (n instanceof LazilyParsedNumber) {
            try {
                return n.longValue();
            } catch (NumberFormatException ex) {
                return n.doubleValue();
            }
        } else {
            // already resolved.
            return n;
        }
    }
}
