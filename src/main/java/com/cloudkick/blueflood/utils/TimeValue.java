package com.cloudkick.blueflood.utils;

import java.util.concurrent.TimeUnit;

public class TimeValue {
    private final TimeUnit unit;
    private final long value;

    public TimeValue(long value, TimeUnit unit) {
        this.value = value;
        this.unit = unit;
    }

    public long getValue() {
        return this.value;
    }

    public TimeUnit getUnit() {
        return this.unit;
    }

    public long toDays() {
        return this.unit.toDays(this.value);
    }

    public long toHours() {
        return this.unit.toHours(this.value);
    }

    public long toMinutes() {
        return this.unit.toMinutes(this.value);
    }

    public long toSeconds() {
        return this.unit.toSeconds(this.value);
    }

    public long toMillis() {
        return this.unit.toMillis(this.value);
    }

    public long toMicros() {
        return this.unit.toMicros(this.value);
    }

    public String toString() {
        return String.format("%s %s", String.valueOf(this.getValue()), unit.name());
    }
}