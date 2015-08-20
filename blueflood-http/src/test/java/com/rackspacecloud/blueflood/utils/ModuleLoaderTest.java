package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.EventsIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.junit.Assert;
import org.junit.Test;


public class ModuleLoaderTest {

    @Test
    public void getInstanceShouldReturnCorrectInstance() throws Exception {

        Configuration.getInstance().setProperty("DISCOVERY_MODULES", "com.rackspacecloud.blueflood.io.ElasticIO");
        Object dm = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);
        Assert.assertTrue("ModuleLoader did not return DiscoveryIO instance for DISCOVERY_MODULES and instead returned: " + dm.getClass(),
                dm instanceof DiscoveryIO);

        Configuration.getInstance().setProperty("EVENTS_MODULES", "com.rackspacecloud.blueflood.io.EventElasticSearchIO");
        Object em = ModuleLoader.getInstance(EventsIO.class, CoreConfig.EVENTS_MODULES);
        Assert.assertTrue("ModuleLoader did not return EventsIO instance for EVENTS_MODULES and instead returned: " + em.getClass(),
                em instanceof EventsIO);

        Assert.assertFalse("ModuleLoader returned DiscoveryIO instance for EVENTS_MODULES",
                (ModuleLoader.getInstance(EventsIO.class, CoreConfig.EVENTS_MODULES)) instanceof DiscoveryIO);

        Assert.assertFalse("ModuleLoader returned EventsIO instance for DISCOVERY_MODULES",
                (ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES)) instanceof EventsIO);

    }

}
