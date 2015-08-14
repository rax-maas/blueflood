package com.rackspacecloud.blueflood.types;

import java.util.Arrays;

public class BluefloodSet {
    private String name;
    private String[] values;

    public String getName() {
        return name;
    }

    public String[] getValues() {
        return Arrays.copyOf(values, values.length, String[].class);
    }
}

