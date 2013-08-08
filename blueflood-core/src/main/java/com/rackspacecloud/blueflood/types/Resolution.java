package com.rackspacecloud.blueflood.types;

public enum Resolution {
    FULL(0),
    MIN5(1),
    MIN20(2),
    MIN60(3),
    MIN240(4),
    MIN1440(5);

    private final int value;

    private Resolution(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static Resolution fromString(String name) {
        return Resolution.valueOf(name.toUpperCase());
    }
}
