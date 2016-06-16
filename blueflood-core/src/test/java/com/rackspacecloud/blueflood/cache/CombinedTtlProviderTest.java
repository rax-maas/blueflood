package com.rackspacecloud.blueflood.cache;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.TtlConfig;
import com.rackspacecloud.blueflood.types.RollupType;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CombinedTtlProviderTest {

    private ConfigTtlProvider configProvider;
    private CombinedTtlProvider combinedProvider;

    @Before
    public void setUp() {
        Properties props = new Properties();
        props.setProperty(TtlConfig.RAW_METRICS_TTL.toString(), "5");
        props.setProperty(TtlConfig.STRING_METRICS_TTL.toString(), "364");
        this.configProvider = new ConfigTtlProvider(Configuration.getTestInstance(props));
        this.combinedProvider = new CombinedTtlProvider(configProvider, SafetyTtlProvider.getInstance());
    }
    @Test
    public void testGetTTL() throws Exception {
        assertTrue(combinedProvider.getTTL("foo", Granularity.FULL, RollupType.BF_BASIC).isPresent());
        assertTrue(combinedProvider.getTTL("foo", Granularity.MIN_5, RollupType.BF_BASIC).isPresent());
        assertFalse(configProvider.getTTL("foo", Granularity.MIN_5, RollupType.BF_BASIC).isPresent());
        assertEquals(combinedProvider.getTTL("foo", Granularity.MIN_5, RollupType.BF_BASIC).get(),
                SafetyTtlProvider.getInstance().getTTL("foo", Granularity.MIN_5, RollupType.BF_BASIC).get());
    }
}