package com.rackspacecloud.blueflood.exceptions;

import com.rackspacecloud.blueflood.exceptions.IncomingMetricException;
import com.rackspacecloud.blueflood.types.Locator;

public class IncomingTypeException extends IncomingMetricException {
    private final Locator locator;
    private final String oldType;
    private final String newType;
    
    public IncomingTypeException(Locator locator, String oldType, String newType) {
        super(String.format("Detected type change for %s %s->%s", locator.toString(), oldType, newType));
        this.locator = locator;
        this.oldType = oldType;
        this.newType = newType;
    }
}
