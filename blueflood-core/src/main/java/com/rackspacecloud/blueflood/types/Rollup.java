package com.rackspacecloud.blueflood.types;

import java.io.IOException;
import java.util.List;

abstract public class Rollup {
    public static enum Type {
        HISTOGRAM("HISTOGRAM"),
        BASIC_STATS("BASIC_STATS");

        private String name;

        private Type(String name) {
            this.name = name;
        }
    }

    abstract public void compute(List<Object> input) throws IOException;

    protected void validateInputsAreSame(List<Object> input) throws IOException {
        Class klass = input.get(0).getClass();
        for (Object item : input) {
            if (!item.getClass().equals(klass)) {
                throw new IOException("Not all inputs are of same type");
            }
        }
    }

    public static Rollup buildRollupFromInputData(List<Object> input, Type rollupType) throws IOException {
        if (rollupType.equals(Type.BASIC_STATS)) {
            return BasicRollup.buildRollupFromInputData(input);
        } else {
            throw new IOException("No other rollup type implemented");
        }
    }
}
