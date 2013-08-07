package com.rackspacecloud.blueflood.inputs.handlers;

public interface ScribeHandlerMBean {
    public int getQueuedWriteCount();
    public int getInFlightWriteCount();
    public int getWriteConcurrency();
    public void setWriteConcurrency(int i);
}
