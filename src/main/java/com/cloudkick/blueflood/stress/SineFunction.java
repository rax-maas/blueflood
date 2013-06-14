package com.cloudkick.blueflood.stress;

class SineFunction {
    long zeroTime;
    long periodMs;
    double yScale;
    
    private SineFunction(long zeroTime, long periodMs, long yMax, long yMin) {
        // assert yMax > yMin
        this.zeroTime = zeroTime;
        this.periodMs = periodMs;
        yScale = (yMax - yMin) / 2d;
    }
    
    protected final double nextValue(long time) {
        time -= zeroTime;
        time %= periodMs;
        return yScale * Math.sin(time * 2 * Math.PI / periodMs) + yScale;
    }
    
    static class DoubleFunction extends SineFunction implements Function<Double> {
        DoubleFunction(long zeroTime, long periodMs, long yMax, long yMin) {
            super(zeroTime, periodMs, yMax, yMin);
        }

        public Double get(long time) {
            return this.nextValue(time);
        }
    }
    
    static class IntFunction extends SineFunction implements Function<Integer> {
        IntFunction(long zeroTime, long periodMs, long yMax, long yMin) {
            super(zeroTime, periodMs, yMax, yMin);
        }

        public Integer get(long time) {
            return Double.valueOf(this.nextValue(time)).intValue();
        }
    }
}
