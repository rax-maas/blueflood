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

import com.rackspacecloud.blueflood.io.Constants;

/**
 * I implemented this on a transatlantic flight, in a benadryl-induced haze.
 * <p/>
 * The goal was to end up with an Average class that 1) didn't need to know much about its type ahead of time (lest we
 * store type in the locator, but that would have other implications), 2) avoided potentially large summations.
 */
public class Average extends AbstractRollupStat {
    private long longRemainder = 0;
    private long count = 0;

    public Average() {
        super();

    }

    @SuppressWarnings("unused") // used by Jackson
    public Average(long value) {
        this();
        this.setLongValue(value);
    }

    @SuppressWarnings("unused") // used by Jackson
    public Average(double value) {
        this();
        this.setDoubleValue(value);
    }

    public Average(int count, Object value) {
        this();

        if (value instanceof Long)
            setLongValue((Long)value);
        else if (value instanceof Integer)
            setLongValue(((Integer)value).longValue());
        else if (value instanceof Double)
            setDoubleValue((Double)value);
        else if (value instanceof Float)
            setDoubleValue(((Float)value).doubleValue());
        else
            throw new RuntimeException(String.format("Unexpected type: %s", value.getClass().getName()));
        this.count = count;
    }

    //
    // long methods.
    //

    public void add(Long input) {
        count++;
        final long longAvgUntilNow = toLong();

        // accuracy could be improved by using summation+division until either count or sum reached a certain level.
        setLongValue(toLong() + ((input + longRemainder - longAvgUntilNow) / count));
        longRemainder = (input + longRemainder - longAvgUntilNow) % count;
    }

    public void addBatch(Long input, long dataPoints) {
        for (long i = 0; i < dataPoints; i++) {
            add(input);
        }
    }

    //
    // double methods.
    //

    public void add(Double input) {
        this.setDoubleValue(toDouble() + ((input - toDouble()) / ++count));
    }

    public void addBatch(Double input, long dataPoints) {
        // if my maths were better, I would know the decay function that would give me the right value.
        for (long i = 0; i < dataPoints; i++) {
            add(input);
        }
    }

    //
    // common methods
    //

    @Override
    void handleFullResMetric(Object number) throws RuntimeException {
        if (number instanceof Long)
            add((Long) number);
        else if (number instanceof Double)
            add((Double)number);
        else if (number instanceof Integer)
            add(((Integer) number).longValue());
        else throw new RuntimeException("Unexpected type to average: " + number.getClass().getName());
    }

    @Override
    void handleRollupMetric(IBasicRollup basicRollup) throws RuntimeException {
        AbstractRollupStat other = basicRollup.getAverage();
        if (isFloatingPoint() || other.isFloatingPoint())
            addBatch(other.toDouble(), basicRollup.getCount());
        else
            addBatch(other.toLong(), basicRollup.getCount());
    }

    @Override
    public byte getStatType() {
        return Constants.AVERAGE;
    }
}
