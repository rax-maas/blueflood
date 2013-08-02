package com.cloudkick.blueflood.types;

import com.cloudkick.blueflood.io.Constants;

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
    void handleRollupMetric(Rollup rollup) throws RuntimeException {
        Average other = rollup.getAverage();
        if (isFloatingPoint() || other.isFloatingPoint())
            addBatch(other.toDouble(), rollup.getCount());
        else
            addBatch(other.toLong(), rollup.getCount());
    }

    @Override
    public byte getStatType() {
        return Constants.AVERAGE;
    }
}
