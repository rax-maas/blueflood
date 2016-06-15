/*
 * Copyright 2013 Rackspace
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
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ConfigTtlProviderTest {
    private ConfigTtlProvider ttlProvider;

    @Before
    public void setUp() {
        Properties props = new Properties();
        props.setProperty(TtlConfig.RAW_METRICS_TTL.toString(), "5");
        props.setProperty(TtlConfig.STRING_METRICS_TTL.toString(), "364");
        this.ttlProvider = new ConfigTtlProvider(Configuration.getTestInstance(props));
    }

    @Test
    public void testConfigTtl_valid() throws Exception {
        Assert.assertTrue(new TimeValue(5, TimeUnit.DAYS).equals(
                ttlProvider.getTTL("acFoo", Granularity.FULL, RollupType.BF_BASIC).get()));
    }

    @Test
    public void testConfigTtl_invalid() {
        Assert.assertFalse(ttlProvider.getTTL("acBar", Granularity.FULL, RollupType.SET).isPresent());
    }

    @Test
    public void testConfigTtlForStrings() throws Exception {
        Assert.assertTrue(new TimeValue(364, TimeUnit.DAYS).equals(
                ttlProvider.getTTLForStrings("acFoo").get()));
    }
}