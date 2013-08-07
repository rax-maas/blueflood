package com.rackspacecloud.blueflood.stress;

class ConstantFunction<T> {
    protected final T v;
    private ConstantFunction(T v) {
        this.v = v;
    }
    
    static class IntFunction extends ConstantFunction<Integer> implements Function<Integer> {
        IntFunction(Integer v) {
            super(v);
        }
        public Integer get(long time) {
            return (Integer)v;
        }
    }  
    
    static class DoubleFunction extends ConstantFunction<Double> implements Function<Double> {
        DoubleFunction(Double v) {
            super(v);
        }

        public Double get(long time) {
            return (Double)v;
        }
    }
    
    
}
