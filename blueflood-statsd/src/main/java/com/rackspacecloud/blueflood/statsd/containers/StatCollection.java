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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.StatType;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A collection of stats. we do a little bit of preorganization to make metric generation simpler.
 **/
public class StatCollection {

    // everything
    private Multimap<StatType, Stat> allStats = HashMultimap.create();
    
    // the next few collections capture metrics that occur on multiple lines: currently timers and countes.
    private Multimap<Locator, Stat> timers = HashMultimap.create();
    private Multimap<Locator, Stat> counters = HashMultimap.create();
    
    public void add(Stat stat) {
        allStats.put(stat.getType(), stat);
        if (stat.getType() == StatType.TIMER)
            timers.put(stat.getLocator(), stat);
        else if (stat.getType() == StatType.COUNTER)
            counters.put(stat.getLocator(), stat);
    }
    
    public Collection<Stat> getStats(StatType type) {
        return allStats.get(type);
    }
    
    public Map<Locator, Collection<Stat>> getTimerStats() {
        return Collections.unmodifiableMap(timers.asMap());
    }
    
    public Map<Locator, Collection<Stat>> getCounterStats() {
        return Collections.unmodifiableMap(counters.asMap());
    }
    
    // in non-legacy mode, counters (comprised of two lines) are rendered as timers.
    // detect those and convert them to a COUNTER pair. We previously threw away the rate component.
    // todo: In the future, it will be part of the counter.
    public static void renderTimersAsCounters(StatCollection stats) {
        Set<Locator> toRemove = new HashSet<Locator>();
        for (Locator locator : stats.timers.keySet()) {
            if (stats.timers.get(locator).size() == 2) {
                toRemove.add(locator);
            }
        }
        
        for (Locator locator : toRemove) {
            Collection<Stat> components = stats.timers.removeAll(locator);
            // one is a counter, the other is a unknown.
            for (Stat stat : components) {
                stats.allStats.remove(stat.getType(), stat);
                stat.getLabel().setType(StatType.COUNTER);
                stats.counters.put(locator, stat);
                stats.allStats.put(StatType.COUNTER, stat);
            }
        }
    }
    
    // used in tests.
    public Iterable<Stat> iterableUnsafe() {
        return new HashSet<Stat>(allStats.values());
    }
}
