package com.rackspacecloud.blueflood.statsd;

import junit.framework.Assert;
import org.junit.Test;

public class StatParserTest { 
    
    @Test
    public void testDoubleGrok() {
        Assert.assertEquals(21.3456d, Stat.grokValue("21.3456"));
    }
    
    @Test
    public void testLongGrok() {
        Assert.assertEquals(12345l, Stat.grokValue("12345"));
    }
}
