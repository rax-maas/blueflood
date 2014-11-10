package com.rackspacecloud.blueflood.rollup;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Ensuring that the slot key works as intended.
 */
public class SlotKeyTest {
    @Test
    public void test_parse() {
      SlotKey.parse("metrics_1440m,10,A");
    }
}