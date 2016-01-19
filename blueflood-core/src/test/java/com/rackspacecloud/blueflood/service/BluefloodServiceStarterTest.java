package com.rackspacecloud.blueflood.service;

import static junit.framework.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class BluefloodServiceStarterTest {

    @Test
    public void testStartupWithDefaultConfig() {
        String[] args = new String[0];
        BluefloodServiceStarter.main(args);

        assertNotNull(args);
    }
}
