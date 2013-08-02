package com.cloudkick.blueflood.exceptions;

import com.cloudkick.blueflood.exceptions.IncomingMetricException;
import com.cloudkick.blueflood.types.Locator;

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
