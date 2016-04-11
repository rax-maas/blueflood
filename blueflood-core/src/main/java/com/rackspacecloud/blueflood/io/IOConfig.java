/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This is a helper class that deals with reading/computing configuration
 * related to IO classes.
 */
public class IOConfig {

    private static IOConfig INSTANCE = new IOConfig();

    public static IOConfig singleton() { return INSTANCE; }

    private final Configuration config = Configuration.getInstance();

    /**
     * Retrieves the set of unique Cassandra hosts from configuration file.
     * If a single host appears multiple times in the configuration, only one will
     * be listed.
     *
     * @return set of unique Cassandra hosts
     */
    public Set<String> getUniqueHosts() {
        Set<String> uniqueHosts = new HashSet<String>();
        Collections.addAll(uniqueHosts, config.getStringProperty(CoreConfig.CASSANDRA_HOSTS).split(","));
        return uniqueHosts;
    }

    /**
     * Retrieves a set of unique Cassandra hosts with Binary Transport protocol
     * enabled from configuration file. This may or may not be different than
     * {@link #getUniqueHosts()}
     *
     * @return set of unique Cassandra hosts with binary transport protocol enabled
     */
    protected Set<String> getUniqueBinaryTransportHosts() {
        Set<String> uniqueHosts = new HashSet<String>();
        Collections.addAll(uniqueHosts, config.getStringProperty(CoreConfig.CASSANDRA_BINXPORT_HOSTS).split(","));
        return uniqueHosts;
    }

    /**
     * Retrieves a set of unique Cassandra hosts with Binary Transport protocol
     * enabled from configuration file as a set of {@link java.net.InetSocketAddress}
     * objects.
     *
     * @return set of InetSocketAddress objects containing unique Cassandra hosts
     * with binary transport protocol enabled
     */
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

    /**
     * Calculates the number of max connections per Cassandra hosts
     *
     * @param numHosts the number of Cassandra hosts in the cluster
     * @return the number of max connections
     */
    public int getMaxConnPerHost(int numHosts) {
        int maxConns = config.getIntegerProperty(CoreConfig.MAX_CASSANDRA_CONNECTIONS);
        return maxConns / numHosts + (maxConns % numHosts == 0 ? 0 : 1);
    }

    public int getInitialConn() {
        return config.getIntegerProperty(CoreConfig.INITIAL_CASSANDRA_CONNECTIONS);
    }

    /**
     * @return the Cassandra request timeout
     */
    public int getRequestTimeout() {
        return config.getIntegerProperty(CoreConfig.CASSANDRA_REQUEST_TIMEOUT);
    }

    // prevent people from instantiating directly
    private IOConfig() {
    }
}
