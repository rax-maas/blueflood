package com.rackspacecloud.blueflood.types;

public class SingleValueRollup implements Rollup {
    
    // numSamples is used primarily for testing.
    protected transient int numSamples = 0;
    
    private Number value;
    
    public SingleValueRollup() {
        this.value = 0L;
    }
    
    public SingleValueRollup withValue(Number value) {
        this.value = value;
        return this;
    }
    
    public Number getValue() { return value; }
    
    public int getNumSamplesUnsafe() { return numSamples; }
}
