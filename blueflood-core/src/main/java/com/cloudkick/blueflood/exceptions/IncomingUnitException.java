package com.cloudkick.blueflood.exceptions;

import com.cloudkick.blueflood.exceptions.IncomingMetricException;
import com.cloudkick.blueflood.types.Locator;

public class IncomingUnitException extends IncomingMetricException {
    private final Locator locator;
    private final String oldUnit;
    private final String newUnit;

    public IncomingUnitException(Locator locator, String oldUnit, String newUnit) {
        super(String.format("Detected unit change for %s %s->%s", locator.toString(), oldUnit, newUnit));
        this.locator = locator;
        this.oldUnit = oldUnit;
        this.newUnit = newUnit;
    }
}
