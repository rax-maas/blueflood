package com.rackspacecloud.blueflood.cache;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.RollupType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SafetyTtlProviderTest {
    private SafetyTtlProvider ttlProvider;

    @Before
    public void instantiationTest() {
        ttlProvider = new SafetyTtlProvider();
    }

    @Test
    public void testAlwaysPresent() {
        Assert.assertTrue(ttlProvider.getTTL("test", Granularity.FULL, RollupType.NOT_A_ROLLUP).isPresent());
    }
}