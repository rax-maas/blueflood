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

package com.rackspacecloud.blueflood.utils;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AtomicCountingSet<T> {
    private ConcurrentMap<T, AtomicInteger> keyedCounter;

    public AtomicCountingSet() {
        this.keyedCounter = new ConcurrentHashMap<T, AtomicInteger>();
    }

    public void increment(T key) {
        AtomicInteger count = keyedCounter.get(key);

        if (count != null) {
            count.incrementAndGet();
        } else {
            count = keyedCounter.putIfAbsent(key, new AtomicInteger(1));
            if (count != null) {         // if we don't get back null, some other thread squeezed in
                count.incrementAndGet();
            }
        }
    }

    public void decrement(T key) {
        AtomicInteger count = keyedCounter.get(key);

        if (count != null) {
            count.decrementAndGet();
            keyedCounter.remove(key, 0);   // remove only if the value is zero
        }
    }

    public boolean contains(T key) {
         return keyedCounter.containsKey(key) && (keyedCounter.get(key).get() > 0);
    }

    public int getCount(T key) {
        AtomicInteger count = keyedCounter.get(key);
        return (count == null) ? 0 : count.get();
    }
    
    public Set<T> asSet() {
        return keyedCounter.keySet();
    }
}
