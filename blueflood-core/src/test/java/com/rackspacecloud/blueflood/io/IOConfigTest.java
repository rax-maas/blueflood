package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetSocketAddress;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

/**
 *  * This is the test class for {@link com.rackspacecloud.blueflood.io.IOConfig}
 */
@PrepareForTest({Configuration.class})
@RunWith(PowerMockRunner.class)
public class IOConfigTest {

    private static Configuration mockConfiguration;

    @BeforeClass
    public static void setup() throws Exception {
        // this is how you mock singleton whose instance is declared
        // as private static final, as the case for Configuration.class
        PowerMockito.suppress(PowerMockito.constructor(Configuration.class));
        mockConfiguration = PowerMockito.mock(Configuration.class);
        PowerMockito.mockStatic(Configuration.class);
        when(Configuration.getInstance()).thenReturn(mockConfiguration);
    }

    @Test
    public void testMultipleCassandraHostsWithPort() {
        IOConfig ioConfig = IOConfig.singleton();
        String expectedHostsStr = "10.10.10.10:9001,11.11.11.11:9002";
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_HOSTS))).thenReturn(expectedHostsStr);

        Set<String> hosts = ioConfig.getUniqueHosts();
        assertEquals("Number of hosts", 2, hosts.size());
    }

    @Test
    public void testMultipleNativeCassandraHostsWithPort() {
        IOConfig ioConfig = IOConfig.singleton();
        String expectedHostsStr = "10.10.10.10:9001,11.11.11.11:9002";
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_BINXPORT_HOSTS))).thenReturn(expectedHostsStr);

        Set<String> hosts = ioConfig.getUniqueBinaryTransportHosts();
        assertEquals("Number of hosts", 2, hosts.size());
    }

    @Test
    public void testDuplicateMultipleNativeCassandraHostsWithPort() {
        IOConfig ioConfig = IOConfig.singleton();
        String expectedHostsStr = "10.10.10.10:9001,11.11.11.11:9002,10.10.10.10:9001,11.11.11.11:9003";
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_BINXPORT_HOSTS))).thenReturn(expectedHostsStr);

        Set<String> hosts = ioConfig.getUniqueBinaryTransportHosts();
        assertEquals("Number of hosts", 3, hosts.size());
    }

    @Test
    public void testMultipleNativeCassandraHostsWithPortToInetAddresses() {
        IOConfig ioConfig = IOConfig.singleton();
        String expectedHostsStr = "10.10.10.10:9001,11.11.11.11:9002";
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_BINXPORT_HOSTS))).thenReturn(expectedHostsStr);

        Set<InetSocketAddress> hosts = ioConfig.getUniqueBinaryTransportHostsAsInetSocketAddresses();
        assertEquals("Number of hosts", 2, hosts.size());
    }

    @Test
    public void testNativeCassandraHostsWithoutPortToInetAddresses() {
        IOConfig ioConfig = IOConfig.singleton();
        String expectedHostsStr = "10.10.10.10,11.11.11.11";
        when(mockConfiguration.getIntegerProperty(eq(CoreConfig.CASSANDRA_BINXPORT_PORT))).thenReturn(1111);
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_BINXPORT_HOSTS))).thenReturn(expectedHostsStr);

        Set<InetSocketAddress> hosts = ioConfig.getUniqueBinaryTransportHostsAsInetSocketAddresses();
        assertEquals("Number of hosts", 2, hosts.size());
        for (InetSocketAddress host: hosts) {
            assertEquals("Port is default 1111", 1111, host.getPort());
        }
    }

    @Test
    public void testNativeCassandraHostsWithPortToInetAddresses() {
        IOConfig ioConfig = IOConfig.singleton();
        String expectedHostsStr = "10.10.10.10:9001,11.11.11.11:9002";
        when(mockConfiguration.getIntegerProperty(eq(CoreConfig.CASSANDRA_BINXPORT_PORT))).thenReturn(1111);
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_BINXPORT_HOSTS))).thenReturn(expectedHostsStr);

        Set<InetSocketAddress> hosts = ioConfig.getUniqueBinaryTransportHostsAsInetSocketAddresses();
        assertEquals("Number of hosts", 2, hosts.size());
        for (InetSocketAddress host: hosts) {
            assertNotSame("Port is not default 1111", 1111, host.getPort());
        }
    }

    @Test
    public void testConnPerHosts() {
        IOConfig ioConfig = IOConfig.singleton();

        when(mockConfiguration.getIntegerProperty(eq(CoreConfig.MAX_CASSANDRA_CONNECTIONS))).thenReturn(70);
        assertEquals("Connection per hosts", 70 / 5, ioConfig.getMaxConnPerHost(5));
    }

    @Test
    public void testConnPerHostsMoreHostsThanCassandraConn() {
        IOConfig ioConfig = IOConfig.singleton();

        // we have more hosts than max Cassandra connections,
        // make sure we don't get 0 or fractions
        when(mockConfiguration.getIntegerProperty(eq(CoreConfig.MAX_CASSANDRA_CONNECTIONS))).thenReturn(70);
        assertEquals("Connection per hosts", 1, ioConfig.getMaxConnPerHost(100));
    }
}
