package com.rackspacecloud.blueflood.exceptions;

public abstract class IncomingMetricException extends Exception {
    public IncomingMetricException(String message) {
        super(message);
    }
}
