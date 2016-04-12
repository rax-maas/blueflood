package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.DummyDiscoveryIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;



public class ModuleLoaderTest {

    @Before
    public void setUp() {
        Configuration.getInstance().clearProperty(CoreConfig.DISCOVERY_MODULES);
        ModuleLoader.clearCache();
    }

    @After
    public void tearDown() {
        Configuration.getInstance().clearProperty(CoreConfig.DISCOVERY_MODULES);
        ModuleLoader.clearCache();
    }

    @Test
    public void singleClassYieldsThatClass() {

        // given
        Configuration.getInstance().setProperty(
                CoreConfig.DISCOVERY_MODULES,
                "com.rackspacecloud.blueflood.utils.DummyDiscoveryIO3");

        // when
        Object loadedModule = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

        // then
        Assert.assertNotNull(loadedModule);
        Assert.assertEquals(DummyDiscoveryIO3.class, loadedModule.getClass());
    }

    @Test
    public void singleClassInDifferentPackageYieldsThatClass() {

        // given
        Configuration.getInstance().setProperty(
                CoreConfig.DISCOVERY_MODULES,
                "com.rackspacecloud.blueflood.io.DummyDiscoveryIO");

        // when
        Object loadedModule = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

        // then
        Assert.assertNotNull(loadedModule);
        Assert.assertEquals(DummyDiscoveryIO.class, loadedModule.getClass());
    }

    @Test
    public void emptyStringYieldsNull() {

        // given
        Configuration.getInstance().setProperty(CoreConfig.DISCOVERY_MODULES, "");

        // when
        Object loadedModule = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

        // then
        Assert.assertNull(loadedModule);
    }

    @Test(expected=RuntimeException.class)
    public void multipleClassesCauseAnException() {

        // given
        Configuration.getInstance().setProperty(
                CoreConfig.DISCOVERY_MODULES,
                "com.rackspacecloud.blueflood.io.DummyDiscoveryIO,com.rackspacecloud.blueflood.io.DummyDiscoveryIO2");

        // when
        Object loadedModule = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

        // then
        // an exception is thrown
    }

    @Test
    public void nonExistentClassYieldsNull() {

        // given
        Configuration.getInstance().setProperty(
                CoreConfig.DISCOVERY_MODULES,
                "com.rackspacecloud.blueflood.NonExistentClass");

        // when
        Object loadedModule = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

        // then
        Assert.assertNull(loadedModule);
    }

    @Test
    public void callingTwiceYieldsTheSameObjectBothTimes() {

        // given
        Configuration.getInstance().setProperty(
                CoreConfig.DISCOVERY_MODULES,
                "com.rackspacecloud.blueflood.io.DummyDiscoveryIO");

        Object loadedModule1 = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

        // precondition
        Assert.assertNotNull(loadedModule1);
        Assert.assertEquals(DummyDiscoveryIO.class, loadedModule1.getClass());

        // when
        Object loadedModule2 = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

        // then
        Assert.assertNotNull(loadedModule2);
        Assert.assertSame(loadedModule1, loadedModule2);
    }

    @Test
    public void givingDifferentKeysButSameClassYieldsTwoSeparateObjects() {

        // given
        Configuration.getInstance().setProperty(
                CoreConfig.DISCOVERY_MODULES,
                "com.rackspacecloud.blueflood.io.DummyDiscoveryIO");
        Configuration.getInstance().setProperty(
                CoreConfig.ENUMS_DISCOVERY_MODULES,
                "com.rackspacecloud.blueflood.io.DummyDiscoveryIO");

        // when
        Object loadedModule1 = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

        // then
        Assert.assertNotNull(loadedModule1);
        Assert.assertEquals(DummyDiscoveryIO.class, loadedModule1.getClass());

        // when
        Object loadedModule2 = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.ENUMS_DISCOVERY_MODULES);

        // then
        Assert.assertNotNull(loadedModule2);
        Assert.assertEquals(DummyDiscoveryIO.class, loadedModule1.getClass());
        Assert.assertNotSame(loadedModule1, loadedModule2);
    }

    @Test
    public void callingTwiceWithSameKeyButDifferentClassesYieldsTheSameObjectBothTimes() {

        // TODO: This functionality is extremely misleading. It's probably not
        // what was originally intended, anyways. Basically, if we pass the
        // same CoreConfig enum field a second time to getInstance after
        // changing the configuration value for that key, ModuleLoader will
        // still give us back the object that it loaded the first time. This is
        // the case even if we specify completely incompatible classes each
        // time, which could lead to runtime casting exceptions.

        // given
        Configuration.getInstance().setProperty(
                CoreConfig.DISCOVERY_MODULES,
                "com.rackspacecloud.blueflood.io.DummyDiscoveryIO");

        Object loadedModule1 = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

        // precondition
        Assert.assertNotNull(loadedModule1);
        Assert.assertEquals(DummyDiscoveryIO.class, loadedModule1.getClass());

        // when
        Configuration.getInstance().setProperty(
                CoreConfig.DISCOVERY_MODULES,
                "com.rackspacecloud.blueflood.io.DummyDiscoveryIO2");
        Object loadedModule2 = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

        // then
        Assert.assertNotNull(loadedModule2);
        Assert.assertSame(loadedModule1, loadedModule2);
    }

    @Test
    public void specifyingAnInterfaceYieldsNull() {

        // given
        Configuration.getInstance().setProperty(
                CoreConfig.DISCOVERY_MODULES,
                "com.rackspacecloud.blueflood.io.DiscoveryIO");

        // when
        Object loadedModule = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

        // then
        Assert.assertNull(loadedModule);
    }

    @Test
    public void classWithNoParameterlessConstructorYieldsNull() {

        // given
        Configuration.getInstance().setProperty(
                CoreConfig.DISCOVERY_MODULES,
                "com.rackspacecloud.blueflood.io.DummyDiscoveryIO4");

        // when
        Object loadedModule = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

        // then
        Assert.assertNull(loadedModule); // ClassLoader.loadClass will throw an InstantiationException
    }

    @Test
    public void packagePrivateClassYieldsNull() {

        // given
        Configuration.getInstance().setProperty(
                CoreConfig.DISCOVERY_MODULES,
                "com.rackspacecloud.blueflood.io.DummyDiscoveryIO5");

        // when
        Object loadedModule = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);

        // then
        Assert.assertNull(loadedModule); // Class.newInstance will throw an IllegalAccessException
    }
}
