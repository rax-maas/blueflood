package com.rackspacecloud.blueflood.types;

import com.bigml.histogram.Bin;
import com.bigml.histogram.SimpleTarget;
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
    public void fromStringHistgramsYieldsHistgrams() {
        assertEquals(RollupType.BF_HISTOGRAMS, RollupType.fromString("BF_HISTOGRAMS"));
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
    public void fromRollupHistogramRollupYieldsHistograms() {
        ArrayList<Bin<SimpleTarget>> bins = new ArrayList<Bin<SimpleTarget>>();
        Rollup rollup = new HistogramRollup(bins);
        assertEquals(RollupType.BF_HISTOGRAMS, RollupType.fromRollup(rollup));
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
}
