package com.rackspacecloud.blueflood.stress;

public interface Function<Number> {
    public Number get(long time);
}
