/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.io;

import com.codahale.metrics.*;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.HostStats;
import com.netflix.astyanax.connectionpool.exceptions.*;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InstrumentedConnectionPoolMonitor implements ConnectionPoolMonitor {

    private static final Logger log = LoggerFactory.getLogger(InstrumentedConnectionPoolMonitor.class);
    private Meter operationFailureMeter  = new Meter();
    private Meter operationSuccessMeter  = new Meter();
    private Meter operationFailoverMeter = new Meter();
    private Meter notFoundMeter          = new Meter();

    private Meter connectionCreateMeter  = new Meter();
    private Meter connectionClosedMeter  = new Meter();
    private Meter connectionBorrowMeter  = new Meter();
    private Meter connectionReturnMeter  = new Meter();

    private Meter connectionCreateFailureMeter = new Meter();

    private Meter hostAddedMeter         = new Meter();
    private Meter hostRemovedMeter       = new Meter();
    private Meter hostDownMeter          = new Meter();
    private Meter hostReactivatedMeter   = new Meter();

    private Meter poolExhaustedMeter     = new Meter();
    private Meter operationTimeoutMeter  = new Meter();
    private Meter socketTimeoutMeter     = new Meter();
    private Meter noHostsMeter           = new Meter();

    private Meter unknownErrorMeter      = new Meter();
    private Meter badRequestMeter        = new Meter();
    private Meter interruptedMeter       = new Meter();
    private Meter transportErrorMeter    = new Meter();

    private Gauge<Long> busyConnections = new Gauge<Long>() {
        @Override
        public Long getValue() {
            return getNumBusyConnections();
        }
    };

    public InstrumentedConnectionPoolMonitor() {
        Metrics.getRegistry().registerAll(new ConnectionPoolMonitorStats());
    }

    private class ConnectionPoolMonitorStats implements MetricSet {
        @Override
        public Map<String, Metric> getMetrics() {
            Map<String, Metric> map = new HashMap<String, Metric>();
            Class kls = InstrumentedConnectionPoolMonitor.class; // readability

            map.put(MetricRegistry.name(kls, "Operation Result Failure"), operationFailureMeter);
            map.put(MetricRegistry.name(kls, "Operation Result Success"), operationSuccessMeter);
            map.put(MetricRegistry.name(kls, "Operation Result Failover"), operationFailoverMeter);
            map.put(MetricRegistry.name(kls, "Operation Result Not Found"), notFoundMeter);

            map.put(MetricRegistry.name(kls, "Connection Created"), connectionCreateMeter);
            map.put(MetricRegistry.name(kls, "Connection Closed"), connectionClosedMeter);
            map.put(MetricRegistry.name(kls, "Connection Borrowed"), connectionBorrowMeter);
            map.put(MetricRegistry.name(kls, "Connection Returned"), connectionReturnMeter);

            map.put(MetricRegistry.name(kls, "Connection Creation Failure"), connectionCreateFailureMeter);

            map.put(MetricRegistry.name(kls, "Host Added"), hostAddedMeter);
            map.put(MetricRegistry.name(kls, "Host Removed"), hostRemovedMeter);
            map.put(MetricRegistry.name(kls, "Host Down"), hostDownMeter);
            map.put(MetricRegistry.name(kls, "Host Reactivated"), hostReactivatedMeter);

            map.put(MetricRegistry.name(kls, "Exceptions Pool Exhausted"), poolExhaustedMeter);
            map.put(MetricRegistry.name(kls, "Exceptions Operation Timeout"), operationTimeoutMeter);
            map.put(MetricRegistry.name(kls, "Exceptions Socket Timeout"), socketTimeoutMeter);
            map.put(MetricRegistry.name(kls, "Exceptions No Hosts"), noHostsMeter);

            map.put(MetricRegistry.name(kls, "Exceptions Unknown Error"), unknownErrorMeter);
            map.put(MetricRegistry.name(kls, "Exceptions Bad Request"), badRequestMeter);
            map.put(MetricRegistry.name(kls, "Exceptions Interrupted"), interruptedMeter);
            map.put(MetricRegistry.name(kls, "Exceptions Transport Error"), transportErrorMeter);

            map.put(MetricRegistry.name(kls, "Busy Connections"), busyConnections);
            return Collections.unmodifiableMap(map);
        }
    }


    private void trackError(Host host, Exception reason) {
        if (reason instanceof PoolTimeoutException) {
            this.poolExhaustedMeter.mark();
        }
        else if (reason instanceof TimeoutException) {
            this.socketTimeoutMeter.mark();
        }
        else if (reason instanceof OperationTimeoutException) {
            this.operationTimeoutMeter.mark();
        }
        else if (reason instanceof BadRequestException) {
            this.badRequestMeter.mark();
        }
        else if (reason instanceof NoAvailableHostsException) {
            this.noHostsMeter.mark();
        }
        else if (reason instanceof InterruptedOperationException) {
            this.interruptedMeter.mark();
        }
        else if (reason instanceof HostDownException) {
            this.hostDownMeter.mark();
        }
        else if (reason instanceof TransportException) {
            this.transportErrorMeter.mark();
        }
        else {
            log.error(reason.toString(), reason);
            this.unknownErrorMeter.mark();
        }
    }

    @Override
    public void incOperationFailure(Host host, Exception reason) {
        if (reason instanceof NotFoundException) {
            this.notFoundMeter.mark();
            return;
        }

        this.operationFailureMeter.mark();
        trackError(host, reason);
    }

    public long getOperationFailureCount() {
        return this.operationFailureMeter.getCount();
    }

    @Override
    public void incOperationSuccess(Host host, long latency) {
        this.operationSuccessMeter.mark();
    }

    public long getOperationSuccessCount() {
        return this.operationSuccessMeter.getCount();
    }

    @Override
    public void incConnectionCreated(Host host) {
        this.connectionCreateMeter.mark();
    }

    public long getConnectionCreatedCount() {
        return this.connectionCreateMeter.getCount();
    }

    @Override
    public void incConnectionClosed(Host host, Exception reason) {
        this.connectionClosedMeter.mark();
    }

    public long getConnectionClosedCount() {
        return this.connectionClosedMeter.getCount();
    }

    @Override
    public void incConnectionCreateFailed(Host host, Exception reason) {
        this.connectionCreateFailureMeter.mark();
    }

    public long getConnectionCreateFailedCount() {
        return this.connectionCreateFailureMeter.getCount();
    }

    @Override
    public void incConnectionBorrowed(Host host, long delay) {
        this.connectionBorrowMeter.mark();
    }

    public long getConnectionBorrowedCount() {
        return this.connectionBorrowMeter.getCount();
    }

    @Override
    public void incConnectionReturned(Host host) {
        this.connectionReturnMeter.mark();
    }

    public long getConnectionReturnedCount() {
        return this.connectionReturnMeter.getCount();
    }

    public long getPoolExhaustedTimeoutCount() {
        return this.poolExhaustedMeter.getCount();
    }

    @Override
    public long getSocketTimeoutCount() {
        return this.socketTimeoutMeter.getCount();
    }

    public long getOperationTimeoutCount() {
        return this.operationTimeoutMeter.getCount();
    }

    @Override
    public void incFailover(Host host, Exception reason) {
        this.operationFailoverMeter.mark();
        trackError(host, reason);
    }

    @Override
    public long getFailoverCount() {
        return this.operationFailoverMeter.getCount();
    }

    @Override
    public void onHostAdded(Host host, HostConnectionPool<?> pool) {
        log.info("AddHost: " + host.getHostName());
        this.hostAddedMeter.mark();
    }

    @Override
    public long getHostAddedCount() {
        return this.hostAddedMeter.getCount();
    }

    @Override
    public void onHostRemoved(Host host) {
        log.info("RemoveHost: " + host.getHostName());
        this.hostRemovedMeter.mark();
    }

    @Override
    public long getHostRemovedCount() {
        return this.hostRemovedMeter.getCount();
    }

    @Override
    public void onHostDown(Host host, Exception reason) {
        log.info("Host down: " + host.getIpAddress() + " because ", reason);
        this.hostDownMeter.mark();
    }

    @Override
    public long getHostDownCount() {
        return this.hostDownMeter.getCount();
    }

    @Override
    public void onHostReactivated(Host host, HostConnectionPool<?> pool) {
        log.info("Reactivating " + host.getHostName());
        this.hostReactivatedMeter.mark();
    }

    public long getHostReactivatedCount() {
        return this.hostReactivatedMeter.getCount();
    }

    @Override
    public long getNoHostCount() {
        return this.noHostsMeter.getCount();
    }

    @Override
    public long getUnknownErrorCount() {
        return this.unknownErrorMeter.getCount();
    }

    @Override
    public long getInterruptedCount() {
        return this.interruptedMeter.getCount();
    }

    public long getTransportErrorCount() {
        return this.transportErrorMeter.getCount();
    }

    @Override
    public long getBadRequestCount() {
        return this.badRequestMeter.getCount();
    }

    public long getNumBusyConnections() {
        return this.connectionBorrowMeter.getCount() - this.connectionReturnMeter.getCount();
    }

    public long getNumOpenConnections() {
        return this.connectionCreateMeter.getCount() - this.connectionClosedMeter.getCount();
    }

    @Override
    public long notFoundCount() {
        return this.notFoundMeter.getCount();
    }

    @Override
    public long getHostCount() {
        return getHostAddedCount() - getHostRemovedCount();
    }

    public long getHostActiveCount() {
        return hostAddedMeter.getCount() - hostRemovedMeter.getCount() + hostReactivatedMeter.getCount() - hostDownMeter.getCount();
    }

    public String toString() {
        // Build the complete status string
        return new StringBuilder()
                .append("InstrumentedConnectionPoolMonitor(")
                .append("Connections[" )
                .append( "open="       ).append(getNumOpenConnections())
                .append(",busy="       ).append(getNumBusyConnections())
                .append(",create="     ).append(connectionCreateMeter.getCount())
                .append(",close="      ).append(connectionClosedMeter.getCount())
                .append(",failed="     ).append(connectionCreateFailureMeter.getCount())
                .append(",borrow="     ).append(connectionBorrowMeter.getCount())
                .append(",return=").append(connectionReturnMeter.getCount())
                .append("], Operations[")
                .append("success=").append(operationSuccessMeter.getCount())
                .append(",failure=").append(operationFailureMeter.getCount())
                .append(",optimeout=").append(operationTimeoutMeter.getCount())
                .append(",timeout=").append(socketTimeoutMeter.getCount())
                .append(",failover=").append(operationFailoverMeter.getCount())
                .append(",nohosts=").append(noHostsMeter.getCount())
                .append(",unknown=").append(unknownErrorMeter.getCount())
                .append(",interrupted=").append(interruptedMeter.getCount())
                .append(",exhausted="  ).append(poolExhaustedMeter.getCount())
                .append(",transport="  ).append(transportErrorMeter.getCount())
                .append("], Hosts[")
                .append("add=").append(hostAddedMeter.getCount())
                .append(",remove=").append(hostRemovedMeter.getCount())
                .append(",down=").append(hostDownMeter.getCount())
                .append(",reactivate=").append(hostReactivatedMeter.getCount())
                .append(",active=").append(getHostActiveCount())
                .append("])").toString();
    }

    @Override
    public Map<Host, HostStats> getHostStats() {
        throw new UnsupportedOperationException("Not supported");
    }
}

