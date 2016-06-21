/*
 * Copyright 2016 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.cache;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.TtlConfig;
import com.rackspacecloud.blueflood.types.RollupType;
import org.junit.After;
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
        System.setProperty(TtlConfig.RAW_METRICS_TTL.toString(), "5");
        System.setProperty(TtlConfig.STRING_METRICS_TTL.toString(), "364");

        this.configProvider = ConfigTtlProvider.getInstance();
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

    @After
    public void tearDown() {
        System.clearProperty(TtlConfig.RAW_METRICS_TTL.toString());
        System.clearProperty(TtlConfig.STRING_METRICS_TTL.toString());
    }
}