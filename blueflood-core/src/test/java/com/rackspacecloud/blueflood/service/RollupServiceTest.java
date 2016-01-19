package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.utils.Util;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

public class RollupServiceTest {

    @Test
    public void testRollupServiceWithDefaultConfigs() {

        Configuration config = Configuration.getInstance();
        final Collection<Integer> shards = Collections.unmodifiableCollection(
                Util.parseShards(config.getStringProperty(CoreConfig.SHARDS)));
        final String zkCluster = config.getStringProperty(CoreConfig.ZOOKEEPER_CLUSTER);
        final ScheduleContext rollupContext = "NONE".equals(zkCluster) ?
                new ScheduleContext(System.currentTimeMillis(), shards) :
                new ScheduleContext(System.currentTimeMillis(), shards, zkCluster);

        RollupService service = new RollupService(rollupContext);

        Assert.assertNotNull(service);

        service.run(null, 120000L); // default SCHEDULE_POLL_PERIOD is 60 seconds

        Assert.assertTrue(true);
    }
}
