package com.rackspacecloud.blueflood.types;

import com.bigml.histogram.Bin;
import com.bigml.histogram.SimpleTarget;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class RollupTypeTest {

    @Test
    public void fromStringNullYieldsBasic() {
        assertEquals(RollupType.BF_BASIC, RollupType.fromString(null));
    }

    @Test
    public void fromStringEmptyStringYieldsBasic() {
        assertEquals(RollupType.BF_BASIC, RollupType.fromString(""));
    }

    @Test
    public void fromStringInvalidYieldsBasic() {
        assertEquals(RollupType.BF_BASIC, RollupType.fromString("abcdef"));
    }

    @Test
    public void fromStringBasicYieldsBasic() {
        assertEquals(RollupType.BF_BASIC, RollupType.fromString("BF_BASIC"));
    }

    @Test
    public void fromStringBasicLowercaseYieldsBasic() {
        assertEquals(RollupType.BF_BASIC, RollupType.fromString("bf_basic"));
    }

    @Test
    public void fromStringBasicMixedCaseYieldsBasic() {
        assertEquals(RollupType.BF_BASIC, RollupType.fromString("bF_bAsIc"));
    }

    @Test
    public void fromStringCounterYieldsCounter() {
        assertEquals(RollupType.COUNTER, RollupType.fromString("COUNTER"));
    }

    @Test
    public void fromStringTimeYieldsTimer() {
        assertEquals(RollupType.TIMER, RollupType.fromString("TIMER"));
    }

    @Test
    public void fromStringSetYieldsSet() {
        assertEquals(RollupType.SET, RollupType.fromString("SET"));
    }

    @Test
    public void fromStringGaugeYieldsGauge() {
        assertEquals(RollupType.GAUGE, RollupType.fromString("GAUGE"));
    }

    @Test
    public void fromStringEnumYieldsEnum() {
        assertEquals(RollupType.ENUM, RollupType.fromString("ENUM"));
    }

    @Test
    public void fromStringNotARollupYieldsNotARollup() {
        assertEquals(RollupType.NOT_A_ROLLUP, RollupType.fromString("NOT_A_ROLLUP"));
    }

    @Test
    public void fromRollupBluefloodSetRollupYieldsSet() {
        Rollup rollup = new BluefloodSetRollup();
        assertEquals(RollupType.SET, RollupType.fromRollup(rollup));
    }

    @Test
    public void fromRollupBluefloodTimerRollupYieldsTimer() {
        Rollup rollup = new BluefloodTimerRollup();
        assertEquals(RollupType.TIMER, RollupType.fromRollup(rollup));
    }

    @Test
    public void fromRollupBluefloodCounterRollupYieldsCounter() {
        Rollup rollup = new BluefloodCounterRollup();
        assertEquals(RollupType.COUNTER, RollupType.fromRollup(rollup));
    }

    @Test
    public void fromRollupBluefloodGaugeRollupYieldsGauge() {
        Rollup rollup = new BluefloodGaugeRollup();
        assertEquals(RollupType.GAUGE, RollupType.fromRollup(rollup));
    }

    class MetricImplementsRollup extends Metric implements Rollup {
        public MetricImplementsRollup(Locator locator, Object metricValue, long collectionTime, TimeValue ttl, String unit) {
            super(locator, metricValue, collectionTime, ttl, unit);
        }
        @Override
        public Boolean hasData() {
            return null;
        }
    }

    @Test
    public void fromRollupMetricYieldsBasic() {
        // TODO: This appears to be very wrong. The fromRollup method takes a
        // Rollup argument, but one of the things it checks against is Metric,
        // which does not implement the Rollup interface.

        Rollup rollup = new MetricImplementsRollup(null, 123L, 0, new TimeValue(456L, TimeUnit.SECONDS), null);
        assertEquals(RollupType.BF_BASIC, RollupType.fromRollup(rollup));
    }

    @Test
    public void fromRollupSimpleNumberYieldsNotARollup() {
        Rollup rollup = new SimpleNumber(123L);
        assertEquals(RollupType.NOT_A_ROLLUP, RollupType.fromRollup(rollup));
    }

    @Test
    public void fromRollupBluefloodEnumRollupYieldsEnum() {
        Rollup rollup = new BluefloodEnumRollup();
        assertEquals(RollupType.ENUM, RollupType.fromRollup(rollup));
    }

    class DummyRollup implements Rollup {

        @Override
        public Boolean hasData() {
            return null;
        }

        @Override
        public RollupType getRollupType() {
            return null;
        }
    }

    @Test(expected = Error.class)
    public void fromRollupCustomSubclassCausesError() {
        Rollup rollup = new DummyRollup();

        // when
        RollupType.fromRollup(rollup);

        // then
        // the error is thrown
    }

    @Test
    public void fromRollupTypeClassSimpleNumberYieldsBasic() {
        assertEquals(RollupType.BF_BASIC, RollupType.fromRollupTypeClass(SimpleNumber.class));
    }

    @Test
    public void fromRollupTypeClassBasicRollupYieldsBasic() {
        assertEquals(RollupType.BF_BASIC, RollupType.fromRollupTypeClass(BasicRollup.class));
    }

    @Test
    public void fromRollupTypeClassBluefloodCounterRollupYieldsCounter() {
        assertEquals(RollupType.COUNTER, RollupType.fromRollupTypeClass(BluefloodCounterRollup.class));
    }

    @Test
    public void fromRollupTypeClassBluefloodSetRollupYieldsSet() {
        assertEquals(RollupType.SET, RollupType.fromRollupTypeClass(BluefloodSetRollup.class));
    }

    @Test
    public void fromRollupTypeClassBluefloodTimerRollupYieldsSet() {
        assertEquals(RollupType.TIMER, RollupType.fromRollupTypeClass(BluefloodTimerRollup.class));
    }

    @Test
    public void fromRollupTypeClassBluefloodGaugeRollupYieldsSet() {
        assertEquals(RollupType.GAUGE, RollupType.fromRollupTypeClass(BluefloodGaugeRollup.class));
    }

    @Test
    public void fromRollupTypeClassBluefloodEnumRollupYieldsSet() {
        assertEquals(RollupType.ENUM, RollupType.fromRollupTypeClass(BluefloodEnumRollup.class));
    }

    @Test(expected = Error.class)
    public void fromRollupTypeClassCustomSubclassThrowsError() {
        // when
        RollupType.fromRollupTypeClass(DummyRollup.class);
        // then
        // the error is thrown
    }

    @Test(expected = Error.class)
    public void fromRollupTypeClassMetricThrowsError() {
        // when
        RollupType.fromRollupTypeClass(MetricImplementsRollup.class);
        // then
        // the error is thrown
    }

    @Test
    public void classOfCounterYieldsBluefloodCounterRollup() {
        assertEquals(BluefloodCounterRollup.class, RollupType.classOf(RollupType.COUNTER, null));
    }

    @Test
    public void classOfTimerYieldsBluefloodTimerRollup() {
        assertEquals(BluefloodTimerRollup.class, RollupType.classOf(RollupType.TIMER, null));
    }

    @Test
    public void classOfSetYieldsBluefloodSetRollup() {
        assertEquals(BluefloodSetRollup.class, RollupType.classOf(RollupType.SET, null));
    }

    @Test
    public void classOfGaugeYieldsBluefloodGaugeRollup() {
        assertEquals(BluefloodGaugeRollup.class, RollupType.classOf(RollupType.GAUGE, null));
    }

    @Test
    public void classOfEnumYieldsBluefloodEnumRollup() {
        assertEquals(BluefloodEnumRollup.class, RollupType.classOf(RollupType.ENUM, null));
    }

    @Test
    public void classOfBasicWithFullGranularityYieldsSimpleNumber() {
        assertEquals(SimpleNumber.class, RollupType.classOf(RollupType.BF_BASIC, Granularity.FULL));
    }

    @Test
    public void classOfBasicWithOtherGranularityYieldsBasicRollup() {
        assertEquals(BasicRollup.class, RollupType.classOf(RollupType.BF_BASIC, null));
        assertEquals(BasicRollup.class, RollupType.classOf(RollupType.BF_BASIC, Granularity.MIN_5));
        assertEquals(BasicRollup.class, RollupType.classOf(RollupType.BF_BASIC, Granularity.MIN_20));
        assertEquals(BasicRollup.class, RollupType.classOf(RollupType.BF_BASIC, Granularity.MIN_60));
        assertEquals(BasicRollup.class, RollupType.classOf(RollupType.BF_BASIC, Granularity.MIN_240));
        assertEquals(BasicRollup.class, RollupType.classOf(RollupType.BF_BASIC, Granularity.MIN_1440));
    }

    @Test(expected = IllegalArgumentException.class)
    public void classOfNotARollupThrowsException() {
        // when
        RollupType.classOf(RollupType.NOT_A_ROLLUP, null);
        // then
        // the error is thrown
    }
}
