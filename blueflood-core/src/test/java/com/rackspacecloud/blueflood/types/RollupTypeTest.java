package com.rackspacecloud.blueflood.types;

import org.junit.Test;

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
}
