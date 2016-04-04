package com.rackspacecloud.blueflood.eventemitter;

import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class RollupEventTest {

    Locator locator;
    Rollup rollup;
    String unit;
    String granularity;
    long timestamp;

    RollupEvent event;

    @Before
    public void setUp() throws IOException {

        locator = Locator.createLocatorFromPathComponents("tenant", "a", "b");
        Points<SimpleNumber> points = new Points<SimpleNumber>();
        rollup = Rollup.BasicFromRaw.compute(points);
        unit = "Some init";
        granularity = "Some granularity";
        timestamp = 1451631661000L;     // 2016-01-01 01:01:01 CST

        event = new RollupEvent(locator, rollup, unit, granularity, timestamp);
    }

    @Test
    public void locatorGetsSetInConstructor() {
        Assert.assertSame(locator, event.getLocator());
    }

    @Test
    public void rollupGetsSetInConstructor() {
        Assert.assertSame(rollup, event.getRollup());
    }

    @Test
    public void unitGetsSetInConstructor() {
        Assert.assertEquals(unit, event.getUnit());
    }

    @Test
    public void granularityGetsSetInConstructor() {
        Assert.assertEquals(granularity, event.getGranularityName());
    }

    @Test
    public void timestampGetsSetInConstructor() {
        Assert.assertEquals(timestamp, event.getTimestamp());
    }

    @Test
    public void setUnitChangesUnit() {
        //given
        String unit2= "Some other unit";
        // when
        event.setUnit(unit2);
        // then
        assertEquals(unit2, event.getUnit());
    }
}
