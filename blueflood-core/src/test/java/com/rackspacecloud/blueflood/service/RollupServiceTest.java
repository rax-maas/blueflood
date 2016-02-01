package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.utils.Util;
import org.junit.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

@Ignore
public class RollupServiceTest {

    @Before
    public void setup() throws IOException {
        Configuration.getInstance().init();
    }

    @After
    public void tearDown() throws IOException {
        Configuration.getInstance().init();
    }

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
