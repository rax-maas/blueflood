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

    private boolean compareConfigTtlWithExpectedTtl(TimeValue userTtl, TimeValue expectedTtl) {
        final ConfigTtlProvider ttlProvider = ConfigTtlProvider.getInstance();
        TimeValue configTtl = ttlProvider.getConfigTTLForUserTTL(userTtl);
        return expectedTtl.toSeconds() == configTtl.toSeconds();
    }

    @Test
    public void configTTLForUserTTLReturnsExpectedValues() throws Exception {
        final Configuration config = Configuration.getInstance();

        //Bucket #1
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(0,TimeUnit.SECONDS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_0), TimeUnit.DAYS)));
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(4,TimeUnit.SECONDS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_0), TimeUnit.DAYS)));

        //Bucket #2
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(5,TimeUnit.SECONDS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_1), TimeUnit.DAYS)));
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(14,TimeUnit.SECONDS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_1), TimeUnit.DAYS)));

        //Bucket #3
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(15,TimeUnit.SECONDS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_2), TimeUnit.DAYS)));
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(29,TimeUnit.SECONDS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_2), TimeUnit.DAYS)));

        //Bucket #4
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(30,TimeUnit.SECONDS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_3), TimeUnit.DAYS)));
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(270,TimeUnit.SECONDS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_3), TimeUnit.DAYS)));
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(299,TimeUnit.SECONDS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_3), TimeUnit.DAYS)));

        //Bucket #5
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(5,TimeUnit.MINUTES),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_4), TimeUnit.DAYS)));
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(1100,TimeUnit.SECONDS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_4), TimeUnit.DAYS)));

        //Bucket #6
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(20,TimeUnit.MINUTES),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_5), TimeUnit.DAYS)));
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(1700,TimeUnit.SECONDS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_5), TimeUnit.DAYS)));
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(59,TimeUnit.MINUTES),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_5), TimeUnit.DAYS)));

        //Bucket #7
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(1,TimeUnit.HOURS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_6), TimeUnit.DAYS)));
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(14,TimeUnit.HOURS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_6), TimeUnit.DAYS)));
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(23,TimeUnit.HOURS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_6), TimeUnit.DAYS)));

        //Bucket #8
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(1,TimeUnit.DAYS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_7), TimeUnit.DAYS)));
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(1000,TimeUnit.DAYS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_7), TimeUnit.DAYS)));
        Assert.assertTrue(compareConfigTtlWithExpectedTtl(new TimeValue(999999,TimeUnit.HOURS),new TimeValue(config.getIntegerProperty(TtlConfig.TTL_CONFIG_7), TimeUnit.DAYS)));
    }
}