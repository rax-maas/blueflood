package com.rackspacecloud.blueflood.eventemitter;

import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Rollup;

public class RollupEvent {
    private Locator locator;
    private Rollup rollup;
    private String units;
    private String granularityName;

    public RollupEvent(Locator loc, Rollup rollup, String units, String gran) {
        this.locator = loc;
        this.rollup = rollup;
        this.units = units;
        this.granularityName = gran;
    }

    public Rollup getRollup() {
        return rollup;
    }

    public void setRollup(Rollup rollup) {
        this.rollup = rollup;
    }

    public Locator getLocator() {
        return locator;
    }

    public void setLocator(Locator locator) {
        this.locator = locator;
    }

    public String getUnits() {
        return units;
    }

    public void setUnits(String units) {
        this.units = units;
    }

    public String getGranularityName() {
        return granularityName;
    }

    public void setGranularityName(String granularityName) {
        this.granularityName = granularityName;
    }
}
