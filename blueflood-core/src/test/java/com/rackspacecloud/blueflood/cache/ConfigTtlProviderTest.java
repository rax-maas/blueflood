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

import com.rackspacecloud.blueflood.exceptions.ConfigException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.TtlConfig;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ConfigTtlProviderTest {
    @Test
    public void testConfigTtl() throws Exception {
        final ConfigTtlProvider ttlProvider = ConfigTtlProvider.getInstance();
        final Configuration config = Configuration.getInstance();

        Assert.assertTrue(new TimeValue(config.getIntegerProperty(TtlConfig.RAW_METRICS_TTL), TimeUnit.DAYS).equals(
                ttlProvider.getTTL("acFoo", Granularity.FULL, RollupType.BF_BASIC)));

        // Ask for an invalid combination of granularity and rollup type
        try {
            Assert.assertNull(ttlProvider.getTTL("acBar", Granularity.FULL, RollupType.BF_HISTOGRAMS));
        } catch (ConfigException ex) {
            // pass
        } catch (Exception ex) {
            Assert.fail("Should have thrown a ConfigException.");
        }
    }

    @Test
    public void testConfigTtlForStrings() throws Exception {
        final ConfigTtlProvider ttlProvider = ConfigTtlProvider.getInstance();
        final Configuration config = Configuration.getInstance();

        Assert.assertTrue(new TimeValue(config.getIntegerProperty(TtlConfig.STRING_METRICS_TTL), TimeUnit.DAYS).equals(
                ttlProvider.getTTLForStrings("acFoo")));
    }
}