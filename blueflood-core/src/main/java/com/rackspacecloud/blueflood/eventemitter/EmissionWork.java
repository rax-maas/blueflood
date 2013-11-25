package com.rackspacecloud.blueflood.eventemitter;


public class EmissionWork implements Runnable{
    private String eventName = "rollup";
    private Object[] payload;

    protected EmissionWork(Object... eventObjectsPayload) {
        this.payload = eventObjectsPayload;
    }

    @Override
    public void run() {
        RollupEventEmitter.getEmitterInstance().emit(eventName, payload);
    }
}
