package com.rackspacecloud.blueflood.stress;

// all times are in seconds.
class SawtoothFunction {
    private final long start;
    private final long period;
    
    private SawtoothFunction(long start, long period) {
        this.start = start;
        this.period = period;
    }    
    
    protected final int nextValue(long time) {
        return (int)((time - start) % period);
    }
    
    static class DoubleFunction extends SawtoothFunction implements Function<Double> {
        DoubleFunction(long start, long period) {
            super(start, period);
        }

        public Double get(long time) {
            return (double)nextValue(time);
        }
    }
    
    static class IntFunction extends SawtoothFunction implements Function<Integer> {
        IntFunction(long start, long period) {
            super(start, period);
        }

        public Integer get(long time) {
            return nextValue(time);
        }
    }
}
