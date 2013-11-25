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
import java.util.Map;

/**
 * A collection of stats. we do a little bit of preorganization to make metric generation simpler.
 **/
public class StatsCollection {

    // everything
    private Multimap<StatType, Stat> allStats = HashMultimap.create();
    
    // timers only.
    private Multimap<Locator, Stat> timers = HashMultimap.create();
    
    public void add(Stat stat) {
        allStats.put(stat.getType(), stat);
        if (stat.getType() == StatType.TIMER)
            timers.put(stat.getLocator(), stat);
    }
    
    public Collection<Stat> getStats(StatType type) {
        return allStats.get(type);
    }
    
    public Map<Locator, Collection<Stat>> getTimerStats() {
        return Collections.unmodifiableMap(timers.asMap());
    }
}
