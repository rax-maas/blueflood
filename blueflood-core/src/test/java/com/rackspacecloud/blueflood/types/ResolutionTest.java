package com.rackspacecloud.blueflood.types;

import org.junit.Assert;
import org.junit.Test;

public class ResolutionTest {

    @Test
    public void testThriftResolutionMapsOneToOne() {
        for (Resolution resolution : Resolution.values()) {
            String bfResName = resolution.name();
            String thriftResName = telescope.thrift.Resolution.findByValue(resolution.getValue()).name();
            Assert.assertEquals(bfResName, thriftResName);
        }
    }
}
