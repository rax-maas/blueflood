package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.junit.Assert;
import org.junit.Test;


public class ModuleLoaderTest {

    @Test
    public void getInstanceShouldReturnCorrectInstance() throws Exception{

        try {
            // test that ModuleLoader for DISCOVERY_MODULES loads ElasticIO
            System.setProperty("DISCOVERY_MODULES", "com.rackspacecloud.blueflood.io.ElasticIO");
            Configuration.getInstance().init();
            Object discoveryModule = ModuleLoader.getInstance(ElasticIO.class, CoreConfig.DISCOVERY_MODULES);
            Assert.assertNotNull("discoveryModule should not be null", discoveryModule);
            Assert.assertTrue(
                    "discoveryModule should be instanceof DiscoveryIO, but is " + discoveryModule.getClass(),
                    discoveryModule instanceof DiscoveryIO);
            Assert.assertTrue(
                    "discoveryModule should be instanceof ElasticIO, but is " + discoveryModule.getClass(),
                    discoveryModule instanceof ElasticIO);

            // test that ModuleLoader for ENUMS_DISCOVERY_MODULES loads EnumElasticIO
            System.setProperty("ENUMS_DISCOVERY_MODULES", "com.rackspacecloud.blueflood.io.EnumElasticIO");
            Configuration.getInstance().init();
            Object enumsDiscoveryModule = ModuleLoader.getInstance(EnumElasticIO.class, CoreConfig.ENUMS_DISCOVERY_MODULES);
            Assert.assertNotNull("enumsDiscoveryModule should not be null", enumsDiscoveryModule);
            Assert.assertTrue(
                    "enumsDiscoveryModule should be instanceof DiscoveryIO, but is " + enumsDiscoveryModule.getClass(),
                    enumsDiscoveryModule instanceof DiscoveryIO);
            Assert.assertTrue(
                    "enumsDiscoveryModule should be instanceof EnumElasticIO, but is " + enumsDiscoveryModule.getClass(),
                    enumsDiscoveryModule instanceof EnumElasticIO);

            // test that ModuleLoader for EVENTS_MODULES loads EventElasticSearchIO
            System.setProperty("EVENTS_MODULES", "com.rackspacecloud.blueflood.io.EventElasticSearchIO");
            Configuration.getInstance().init();
            Object eventsModules = ModuleLoader.getInstance(EventsIO.class, CoreConfig.EVENTS_MODULES);
            Assert.assertNotNull("eventsModules should not be null", eventsModules);
            Assert.assertTrue(
                    "eventsModules should be instanceof EventsIO, but is " + eventsModules.getClass(),
                    eventsModules instanceof EventsIO);
            Assert.assertTrue(
                    "eventsModules should be instanceof EventElasticSearchIO, but is " + eventsModules.getClass(),
                    eventsModules instanceof EventElasticSearchIO);

            // test that ModuleLoader did not mixed up the IO classes
            Assert.assertFalse(
                    "ModuleLoader should not returned EventsIO instance for DISCOVERY_MODULES",
                    (ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES)) instanceof EventsIO);

            Assert.assertFalse(
                    "ModuleLoader should not returned EventsIO instance for ENUMS_DISCOVERY_MODULES",
                    (ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.ENUMS_DISCOVERY_MODULES)) instanceof EventsIO);

            Assert.assertFalse(
                    "ModuleLoader should not returned DiscoveryIO instance for EVENTS_MODULES",
                    (ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.EVENTS_MODULES)) instanceof DiscoveryIO);
        } finally {
            System.clearProperty("DISCOVERY_MODULES");
            System.clearProperty("ENUMS_DISCOVERY_MODULES");
            System.clearProperty("EVENTS_MODULES");
        }
    }
}
