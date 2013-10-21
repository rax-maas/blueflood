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

package com.rackspacecloud.blueflood.statsd.containers;

import com.google.common.base.Joiner;
import com.rackspacecloud.blueflood.types.CounterRollup;
import com.rackspacecloud.blueflood.types.GaugeRollup;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.PreaggregatedMetric;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.SetRollup;
import com.rackspacecloud.blueflood.types.TimerRollup;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Conversions {
    private static final Logger log = LoggerFactory.getLogger(Conversions.class);
    private static final TimeValue DEFAULT_TTL = new TimeValue(48, TimeUnit.HOURS);
    
    public static IMetric asMetric(Stat stat) {
        Rollup rollup = null;
        switch (stat.getType()) {
            case SET:
                rollup = new SetRollup().withCount(stat.getValue().longValue());
                return new PreaggregatedMetric(stat.getTimestamp() * 1000, stat.getLocator(), DEFAULT_TTL, rollup);
            case GAUGE:
                rollup = new GaugeRollup().withGauge(stat.getValue());
                return new PreaggregatedMetric(stat.getTimestamp() * 1000, stat.getLocator(), DEFAULT_TTL, rollup);
            case COUNTER:
                rollup = new CounterRollup(1).withCount(stat.getValue().longValue());
                return new PreaggregatedMetric(stat.getTimestamp() * 1000, stat.getLocator(), DEFAULT_TTL, rollup);
            case UNKNOWN:
                return new Metric(stat.getLocator(), stat.getValue(), stat.getTimestamp() * 1000, DEFAULT_TTL, null);   
            default:
                return null; // timer make it in?
        }   
    }
    
    public static IMetric asTimerMetric(Locator locator, Collection<Stat> stats) {
    return new PreaggregatedMetric(
            stats.iterator().next().getTimestamp() * 1000,
            locator,
            DEFAULT_TTL,
            Conversions.statsToTimer(stats)
        );
    }
    
    public static Stat asStat(CharSequence str) {
        String[] parts = str.toString().split(" ", -1);
        return new Stat(parts[0], Stat.Parser.grokValue(parts[1]), Long.parseLong(parts[2]));
    }
    
    // convert to a statsd formatted line.
    public static String asLine(Stat stat) {
        return String.format("%s %s %s", stat.getLabel(), stat.getValue().toString(), Long.toString(stat.getTimestamp()));
    }
    
    // convert a collection of stats to a single timer rollup.
    public static TimerRollup statsToTimer(Collection<Stat> stats) {
        // long sum, double count_ps, Number average, double variance, Number min, Number max, long count
        Map<String, Map<String, Number>> percentiles = new HashMap<String, Map<String, Number>>();
        long sum = 0;
        double countPS = 0;
        Number average= 0;
        double variance= 0;
        Number min = 0;
        Number max = 0;
        long count= 0;
        
        // todo: use a bitfield.
        int remainingRequired = 8;
        
        for (Stat stat : stats) {
            if ("upper".equals(stat.getName())) {
                max = stat.getValue();
                remainingRequired--;
            } else if ("lower".equals(stat.getName())) {
                min = stat.getValue();
                remainingRequired--;
            } else if ("median".equals(stat.getName())) {
                remainingRequired--;
                // median gets thrown out.
            } else if ("mean".equals(stat.getName())) {
                average = stat.getValue();
                remainingRequired--;
            } else if ("sum".equals(stat.getName())) {
                sum = stat.getValue().longValue();
                remainingRequired--;
            } else if ("count_ps".equals(stat.getName())) {
                countPS = stat.getValue().doubleValue();
                remainingRequired--;
            } else if ("count".equals(stat.getName())) {
                count = stat.getValue().longValue();
                remainingRequired--;
            } else if ("std".equals(stat.getName())) {
                variance = Math.pow(stat.getValue().doubleValue(), 2d);
                remainingRequired--;
            } else if (stat.getName() != null && stat.getName().indexOf("_") >= 0){
                String[] pctlParts = stat.getName().split("_", -1);
                Map<String, Number> map = percentiles.get(pctlParts[1]);
                if (map == null) {
                    map = new HashMap<String, Number>();
                    percentiles.put(pctlParts[1], map);
                }
                map.put(pctlParts[0], stat.getValue());
            }
        }
        
        if (remainingRequired > 0) {
            log.debug("Couldn't convert to timer {}", Joiner.on(",").join(stats));
            return null;
        } else {
            TimerRollup rollup = new TimerRollup()
                    .withSum(sum)
                    .withCountPS(countPS)
                    .withAverage(average)
                    .withVariance(variance)
                    .withMinValue(min)
                    .withMaxValue(max)
                    .withCount(count);
            for (Map.Entry<String, Map<String, Number>> entry : percentiles.entrySet()) {
                if (entry.getValue().size() != 3) {
                    log.debug("Invalid percentile {} {}", entry.getKey(), Joiner.on(",").join(entry.getValue().keySet()));
                }
                double pctMean= 0;
                Long pctSum = 0L;
                Number pctUpper = 0;
                for (Map.Entry<String, Number> pct : entry.getValue().entrySet()) {
                    if ("mean".equals(pct.getKey()))
                        pctMean = pct.getValue().doubleValue();
                    else if ("sum".equals(pct.getKey()))
                        pctSum = pct.getValue().longValue();
                    else if ("upper".equals(pct.getKey()))
                        pctUpper = pct.getValue();
                }
                rollup.setPercentile(entry.getKey(), pctMean, pctSum, pctUpper);
            }
            
            return rollup;
        }
    }
    
    public static TypedMetricsCollection asMetrics(StatsCollection stats) {
        TypedMetricsCollection group = new TypedMetricsCollection();
        
        // build simple types.
        for (StatType type : StatType.SIMPLE_TYPES) {
            for (Stat stat : stats.getStats(type)) {
                IMetric metric = Conversions.asMetric(stat);
                group.addMetric(metric);
            }
        }
        
        // build timers.
        for (Map.Entry<Locator, Collection<Stat>> entry : stats.getTimerStats().entrySet()) {
            IMetric metric = Conversions.asTimerMetric(entry.getKey(), entry.getValue());
            group.addMetric(metric);
        }
        
        return group;
    }
}
