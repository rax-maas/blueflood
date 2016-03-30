package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by shin4590 on 3/24/16.
 */
public class IOConfig {

    protected static final Configuration config = Configuration.getInstance();

    protected static IOConfig INSTANCE = new IOConfig();

    public static IOConfig singleton() { return INSTANCE; }

    private IOConfig() {
    }

    public Set<String> getUniqueHosts() {
        Set<String> uniqueHosts = new HashSet<String>();
        Collections.addAll(uniqueHosts, config.getStringProperty(CoreConfig.CASSANDRA_HOSTS).split(","));
        return uniqueHosts;
    }

    protected Set<String> getUniqueBinaryTransportHosts() {
        Set<String> uniqueHosts = new HashSet<String>();
        Collections.addAll(uniqueHosts, config.getStringProperty(CoreConfig.CASSANDRA_BINXPORT_HOSTS).split(","));
        return uniqueHosts;
    }

    public Set<InetSocketAddress> getUniqueBinaryTransportHostsAsInetSocketAddresses() {
        Set<String> hosts = getUniqueBinaryTransportHosts();

        Set<InetSocketAddress> inetAddresses = new HashSet<InetSocketAddress>();
        for (String host: hosts) {
            String[] parts = host.split(":");
            InetSocketAddress inetSocketAddress;
            if ( parts.length == 1 ) {
                inetSocketAddress = new InetSocketAddress(parts[0], config.getIntegerProperty(CoreConfig.CASSANDRA_BINXPORT_PORT));
            } else {
                inetSocketAddress = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
            }
            inetAddresses.add(inetSocketAddress);
        }
        return inetAddresses;
    }

    public int getMaxConnPerHost(int numHosts) {
        int maxConns = config.getIntegerProperty(CoreConfig.MAX_CASSANDRA_CONNECTIONS);
        return maxConns / numHosts + (maxConns % numHosts == 0 ? 0 : 1);
    }

    public int getRequestTimeout() {
        return config.getIntegerProperty(CoreConfig.CASSANDRA_REQUEST_TIMEOUT);
    }
}
