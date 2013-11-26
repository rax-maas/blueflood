package com.rackspacecloud.blueflood.eventemitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollupEmissionWork implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(RollupEmissionWork.class);
    //TODO : Generalize this later for any event
    private String eventName = "rollup";
    private Object[] payload;

    protected RollupEmissionWork(Object... eventObjectsPayload) {
        this.payload = eventObjectsPayload;
    }

    @Override
    public void run() {
        try {
            RollupEventEmitter.getEmitterInstance().emit(eventName, payload);
        } catch (Exception e) {
            log.error("Error encountered while emitting to event ", eventName);
        }
    }
}
