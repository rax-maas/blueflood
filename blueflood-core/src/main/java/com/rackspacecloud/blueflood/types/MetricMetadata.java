package com.rackspacecloud.blueflood.types;

public enum MetricMetadata {
    TYPE (0),
    UNIT (1);

    private final int value;
    MetricMetadata(int value) {
        this.value = value;
    }
    public int value() { return value; }
}
