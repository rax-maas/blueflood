package com.rackspacecloud.blueflood.stress;

import java.util.Random;

class RandomFunction {
    protected final int range;
    protected final Random rand;
    
    private RandomFunction(int range) {
        this.range = range;
        rand = new Random(System.currentTimeMillis());
    }
    
    static class DoubleFunction extends  RandomFunction implements Function<Double> {
        DoubleFunction(int range) {
            super(range);
        }

        public Double get(long time) {
            return rand.nextDouble() * range;
        }
    }
    
    static class IntFunction extends RandomFunction implements Function<Integer> {
        IntFunction(int range) {
            super(range);
        }

        public Integer get(long time) {
            return rand.nextInt(range);
        }
    }
    
    
}
