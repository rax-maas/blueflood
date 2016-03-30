package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.io.astyanax.AstyanaxIO;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxShardStateIO;
import com.rackspacecloud.blueflood.io.datastax.DatastaxShardStateIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * This is the test class for {@link com.rackspacecloud.blueflood.io.IOFactories}
 */
// we ignore MBeans and Metrics stuff because a lot of them have static
// initializer that don't work well when the caller of those classes
// (such as AstyanaxIO) are being mocked
@PowerMockIgnore({"javax.management.*", "com.rackspacecloud.blueflood.utils.Metrics", "com.codahale.metrics.*"})
@PrepareForTest({Configuration.class, AstyanaxIO.class, AstyanaxShardStateIO.class})
@RunWith(PowerMockRunner.class)
@Ignore
public class IOFactoriesTest {

    private static Configuration mockConfiguration;

    @BeforeClass
    public static void setup() throws Exception {
        // this is how you mock singleton whose instance is declared
        // as private static final, as the case for Configuration.class
        PowerMockito.suppress(PowerMockito.constructor(Configuration.class));
        mockConfiguration = PowerMockito.mock(Configuration.class);
        PowerMockito.mockStatic(Configuration.class);
        when(Configuration.getInstance()).thenReturn(mockConfiguration);

        mockAstyanaxIO();
        mockShardStateAstyanaxIO();
    }

    private static AstyanaxIO mockAstyanaxIO() {
        // These are needed because AstyanaxIO class has static initializer that
        // utilizes these configs. If we don't mock these calls, we will get NPE
        // somewhere.
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_HOSTS))).thenReturn("127.0.0.1:9160");
        when(mockConfiguration.getIntegerProperty(eq(CoreConfig.MAX_CASSANDRA_CONNECTIONS))).thenReturn(70);

        // mock the AstyanaxIO itself
        PowerMockito.suppress(PowerMockito.constructor(AstyanaxIO.class));
        AstyanaxIO mockAstyanaxIO = PowerMockito.mock(AstyanaxIO.class);
        PowerMockito.mockStatic(AstyanaxIO.class);
        when(AstyanaxIO.singleton()).thenReturn(mockAstyanaxIO);
        return mockAstyanaxIO;
    }

    private static AstyanaxShardStateIO mockShardStateAstyanaxIO() throws Exception {

        AstyanaxShardStateIO mockShardStateAstyanaxIO = PowerMockito.mock(AstyanaxShardStateIO.class);
        PowerMockito.whenNew(AstyanaxShardStateIO.class).withNoArguments().thenReturn(mockShardStateAstyanaxIO);
        return mockShardStateAstyanaxIO;
    }

    @Test
    public void testNullDriverConfig() throws Exception {

        IOFactories ioFactories = IOFactories.singleton();
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_DRIVER))).thenReturn(null);

        ShardStateIO shardStateIO = ioFactories.getShardStateIO();
        assertTrue("ShardStateIO instance is Astyanax", shardStateIO instanceof AstyanaxShardStateIO);
    }

    @Test
    public void testEmptyStringDriverConfig() throws Exception {

        IOFactories ioFactories = IOFactories.singleton();
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_DRIVER))).thenReturn("");

        ShardStateIO shardStateIO = ioFactories.getShardStateIO();
        assertTrue("ShardStateIO instance is Astyanax", shardStateIO instanceof AstyanaxShardStateIO);
    }

    @Test
    public void testAstyanaxDriverConfig() throws Exception {

        IOFactories ioFactories = IOFactories.singleton();
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_DRIVER))).thenReturn(IOFactories.ASTYANAX_DRIVER);

        ShardStateIO shardStateIO = ioFactories.getShardStateIO();
        assertTrue("ShardStateIO instance is Astyanax", shardStateIO instanceof AstyanaxShardStateIO);
    }

    @Test
    public void testDatastaxDriverConfig() {
        IOFactories ioFactories = IOFactories.singleton();
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_DRIVER))).thenReturn(IOFactories.DATASTAX_DRIVER);

        ShardStateIO shardStateIO = ioFactories.getShardStateIO();
        assertTrue("ShardStateIO instance is Datastax", shardStateIO instanceof DatastaxShardStateIO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDriverConfig() {
        IOFactories ioFactories = IOFactories.singleton();
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_DRIVER))).thenReturn("bogusDriver");

        ioFactories.getShardStateIO();
    }
}
