package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.GenericElasticSearchIO;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.junit.Assert;
import org.junit.Test;


public class ModuleLoaderTest {

    @Test
    public void getInstanceShouldReturnCorrectInstance(){

        System.setProperty("EVENTS_MODULES", "");
        Assert.assertNull(ModuleLoader.getInstance(GenericElasticSearchIO.class, CoreConfig.EVENTS_MODULES));

        System.setProperty("DISCOVERY_MODULES", "com.rackspacecloud.blueflood.io.ElasticIO");
        Assert.assertTrue((ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES)) instanceof com.rackspacecloud.blueflood.io.DiscoveryIO);


    }

}