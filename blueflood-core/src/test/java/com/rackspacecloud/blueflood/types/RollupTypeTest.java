package com.rackspacecloud.blueflood.types;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RollupTypeTest {

    @Test
    public void testFromString() {
        assertEquals(RollupType.BF_BASIC, RollupType.fromString(null));
        assertEquals(RollupType.BF_BASIC, RollupType.fromString(""));
        assertEquals(RollupType.BF_BASIC, RollupType.fromString("abcdef"));

        assertEquals(RollupType.BF_BASIC, RollupType.fromString("BF_BASIC"));
        assertEquals(RollupType.BF_BASIC, RollupType.fromString("bf_basic"));
        assertEquals(RollupType.BF_BASIC, RollupType.fromString("bF_bAsIc"));

        assertEquals(RollupType.COUNTER, RollupType.fromString("COUNTER"));
        assertEquals(RollupType.TIMER, RollupType.fromString("TIMER"));
        assertEquals(RollupType.SET, RollupType.fromString("SET"));
        assertEquals(RollupType.GAUGE, RollupType.fromString("GAUGE"));
        assertEquals(RollupType.ENUM, RollupType.fromString("ENUM"));
        assertEquals(RollupType.BF_HISTOGRAMS, RollupType.fromString("BF_HISTOGRAMS"));

        assertEquals(RollupType.NOT_A_ROLLUP, RollupType.fromString("NOT_A_ROLLUP"));
    }
}
