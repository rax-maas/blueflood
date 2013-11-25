package com.rackspacecloud.blueflood.types;

public class SingleValueRollup implements Rollup {
    
    // numSamples is used primarily for testing.
    protected transient int numSamples = 0;
    
    private Number value;
    
    public SingleValueRollup() {
        this.value = 0L;
    }
    
    public SingleValueRollup withValue(Number value) {
        this.value = promoteToDoubleOrLong(value);
        return this;
    }
    
    public Number getValue() { return value; }
    
    public int getNumSamplesUnsafe() { return numSamples; }
    
    private static Number promoteToDoubleOrLong(Number num) {
        if (num instanceof Float)
            return num.doubleValue();
        else if (num instanceof Integer)
            return num.longValue();
        return num;
    }

    @Override
    public String toString() {
        return String.format("value: %s, samples: %d", value.toString(), numSamples);
    }
}
