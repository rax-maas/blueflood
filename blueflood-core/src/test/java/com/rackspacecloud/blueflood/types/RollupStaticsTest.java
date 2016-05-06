package com.rackspacecloud.blueflood.types;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class RollupStaticsTest {

    @Test
    public void testBasicFromRaw() throws IOException {
        Rollup.Type<SimpleNumber, BasicRollup> factory = Rollup.BasicFromRaw;
        Points<SimpleNumber> points = new Points<SimpleNumber>();
        BasicRollup rollup = factory.compute(points);

        assertNotNull(rollup);
        assertSame(BasicRollup.class, rollup.getClass());  // make sure it's exactly this and not a subtype
    }

    @Test
    public void testBasicFromBasic() throws IOException {
        Rollup.Type<BasicRollup, BasicRollup> factory = Rollup.BasicFromBasic;
        Points<BasicRollup> points = new Points<BasicRollup>();
        BasicRollup rollup = factory.compute(points);

        assertNotNull(rollup);
        assertSame(BasicRollup.class, rollup.getClass());
    }

    @Test
    public void testTimerFromTimer() throws IOException {
        Rollup.Type<BluefloodTimerRollup, BluefloodTimerRollup> factory = Rollup.TimerFromTimer;
        Points<BluefloodTimerRollup> points = new Points<BluefloodTimerRollup>();
        BluefloodTimerRollup rollup = factory.compute(points);

        assertNotNull(rollup);
        assertSame(BluefloodTimerRollup.class, rollup.getClass());
    }

    @Test
    public void testCounterFromRaw() throws IOException {
        Rollup.Type<SimpleNumber, BluefloodCounterRollup> factory = Rollup.CounterFromRaw;
        Points<SimpleNumber> points = new Points<SimpleNumber>();
        BluefloodCounterRollup rollup = factory.compute(points);

        assertNotNull(rollup);
        assertSame(BluefloodCounterRollup.class, rollup.getClass());
    }

    @Test
    public void testEnumFromEnum() throws IOException {
        Rollup.Type<BluefloodEnumRollup, BluefloodEnumRollup> factory = Rollup.EnumFromEnum;
        Points<BluefloodEnumRollup> points = new Points<BluefloodEnumRollup>();
        BluefloodEnumRollup rollup = factory.compute(points);

        assertNotNull(rollup);
        assertSame(BluefloodEnumRollup.class, rollup.getClass());
    }

    @Test
    public void testCounterFromCounter() throws IOException {
        Rollup.Type<BluefloodCounterRollup, BluefloodCounterRollup> factory = Rollup.CounterFromCounter;
        Points<BluefloodCounterRollup> points = new Points<BluefloodCounterRollup>();
        BluefloodCounterRollup rollup = factory.compute(points);

        assertNotNull(rollup);
        assertSame(BluefloodCounterRollup.class, rollup.getClass());
    }

    @Test
    public void testGaugeFromRaw() throws IOException {
        Rollup.Type<SimpleNumber, BluefloodGaugeRollup> factory = Rollup.GaugeFromRaw;
        Points<SimpleNumber> points = new Points<SimpleNumber>();
        BluefloodGaugeRollup rollup = factory.compute(points);

        assertNotNull(rollup);
        assertSame(BluefloodGaugeRollup.class, rollup.getClass());
    }

    @Test
    public void testGaugeFromGauge() throws IOException {
        Rollup.Type<BluefloodGaugeRollup, BluefloodGaugeRollup> factory = Rollup.GaugeFromGauge;
        Points<BluefloodGaugeRollup> points = new Points<BluefloodGaugeRollup>();
        BluefloodGaugeRollup rollup = factory.compute(points);

        assertNotNull(rollup);
        assertSame(BluefloodGaugeRollup.class, rollup.getClass());
    }

    @Test
    public void testSetFromSet() throws IOException {
        Rollup.Type<BluefloodSetRollup, BluefloodSetRollup> factory = Rollup.SetFromSet;
        Points<BluefloodSetRollup> points = new Points<BluefloodSetRollup>();
        BluefloodSetRollup rollup = factory.compute(points);

        assertNotNull(rollup);
        assertSame(BluefloodSetRollup.class, rollup.getClass());
    }
}
