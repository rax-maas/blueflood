package com.rackspacecloud.blueflood.io;

import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.HostStats;
import com.netflix.astyanax.connectionpool.exceptions.*;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InstrumentedConnectionPoolMonitor implements ConnectionPoolMonitor {
    private static final Logger log = LoggerFactory.getLogger(InstrumentedConnectionPoolMonitor.class);
    private Meter operationFailureMeter  = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Operation Result Failure", "Failure", TimeUnit.SECONDS);
    private Meter operationSuccessMeter  = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Operation Result Success", "Success", TimeUnit.SECONDS);
    private Meter operationFailoverMeter = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Operation Result Failover", "Failover", TimeUnit.SECONDS);
    private Meter notFoundMeter          = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Operation Result Not Found", "Not Found", TimeUnit.SECONDS);

    private Meter connectionCreateMeter  = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Connection Created", "Created", TimeUnit.SECONDS);
    private Meter connectionClosedMeter  = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Connection Closed", "Closed", TimeUnit.SECONDS);
    private Meter connectionBorrowMeter  = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Connection Borrowed", "Borrowed", TimeUnit.SECONDS);
    private Meter connectionReturnMeter  = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Connection Returned", "Returned", TimeUnit.SECONDS);

    private Meter connectionCreateFailureMeter = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Connection Creation Failure", "Creation Failure", TimeUnit.SECONDS);

    private Meter hostAddedMeter         = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Host Added", "Added", TimeUnit.SECONDS);
    private Meter hostRemovedMeter       = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Host Removed", "Removed", TimeUnit.SECONDS);
    private Meter hostDownMeter          = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Host Down", "Down", TimeUnit.SECONDS);
    private Meter hostReactivatedMeter   = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Host Reactivated", "Reactivated", TimeUnit.SECONDS);

    private Meter poolExhaustedMeter = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Exceptions Pool Exhausted", "Pool Exhasted", TimeUnit.SECONDS);
    private Meter operationTimeoutMeter  = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Exceptions Operation Timeout", "Operation Timeout", TimeUnit.SECONDS);
    private Meter socketTimeoutMeter     = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Exceptions Socket Timeout", "Socket Timeout", TimeUnit.SECONDS);
    private Meter noHostsMeter           = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Exceptions No Hosts", "No Hosts", TimeUnit.SECONDS);
    private Meter unknownErrorMeter      = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Exceptions Unknown Error", "Unknown Error", TimeUnit.SECONDS);
    private Meter badRequestMeter        = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Exceptions Bad Request", "Bad Request", TimeUnit.SECONDS);
    private Meter interruptedMeter       = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Exceptions Interrupted", "Interrupted", TimeUnit.SECONDS);
    private Meter transportErrorMeter    = Metrics.newMeter(InstrumentedConnectionPoolMonitor.class, "Exceptions Transport Error", "Transport Error", TimeUnit.SECONDS);

    private Gauge<Long> busyConnections = Metrics.newGauge(InstrumentedConnectionPoolMonitor.class, "Busy Connections", new Gauge<Long>() {
        @Override
        public Long value() {
            return getNumBusyConnections();
        }
    });


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
        return this.operationFailureMeter.count();
    }

    @Override
    public void incOperationSuccess(Host host, long latency) {
        this.operationSuccessMeter.mark();
    }

    public long getOperationSuccessCount() {
        return this.operationSuccessMeter.count();
    }

    @Override
    public void incConnectionCreated(Host host) {
        this.connectionCreateMeter.mark();
    }

    public long getConnectionCreatedCount() {
        return this.connectionCreateMeter.count();
    }

    @Override
    public void incConnectionClosed(Host host, Exception reason) {
        this.connectionClosedMeter.mark();
    }

    public long getConnectionClosedCount() {
        return this.connectionClosedMeter.count();
    }

    @Override
    public void incConnectionCreateFailed(Host host, Exception reason) {
        this.connectionCreateFailureMeter.mark();
    }

    public long getConnectionCreateFailedCount() {
        return this.connectionCreateFailureMeter.count();
    }

    @Override
    public void incConnectionBorrowed(Host host, long delay) {
        this.connectionBorrowMeter.mark();
    }

    public long getConnectionBorrowedCount() {
        return this.connectionBorrowMeter.count();
    }

    @Override
    public void incConnectionReturned(Host host) {
        this.connectionReturnMeter.mark();
    }

    public long getConnectionReturnedCount() {
        return this.connectionReturnMeter.count();
    }

    public long getPoolExhaustedTimeoutCount() {
        return this.poolExhaustedMeter.count();
    }

    @Override
    public long getSocketTimeoutCount() {
        return this.socketTimeoutMeter.count();
    }

    public long getOperationTimeoutCount() {
        return this.operationTimeoutMeter.count();
    }

    @Override
    public void incFailover(Host host, Exception reason) {
        this.operationFailoverMeter.mark();
        trackError(host, reason);
    }

    @Override
    public long getFailoverCount() {
        return this.operationFailoverMeter.count();
    }

    @Override
    public void onHostAdded(Host host, HostConnectionPool<?> pool) {
        log.info("AddHost: " + host.getHostName());
        this.hostAddedMeter.mark();
    }

    @Override
    public long getHostAddedCount() {
        return this.hostAddedMeter.count();
    }

    @Override
    public void onHostRemoved(Host host) {
        log.info("RemoveHost: " + host.getHostName());
        this.hostRemovedMeter.mark();
    }

    @Override
    public long getHostRemovedCount() {
        return this.hostRemovedMeter.count();
    }

    @Override
    public void onHostDown(Host host, Exception reason) {
        log.info("Host down: " + host.getIpAddress() + " because ", reason);
        this.hostDownMeter.mark();
    }

    @Override
    public long getHostDownCount() {
        return this.hostDownMeter.count();
    }

    @Override
    public void onHostReactivated(Host host, HostConnectionPool<?> pool) {
        log.info("Reactivating " + host.getHostName());
        this.hostReactivatedMeter.mark();
    }

    public long getHostReactivatedCount() {
        return this.hostReactivatedMeter.count();
    }

    @Override
    public long getNoHostCount() {
        return this.noHostsMeter.count();
    }

    @Override
    public long getUnknownErrorCount() {
        return this.unknownErrorMeter.count();
    }

    @Override
    public long getInterruptedCount() {
        return this.interruptedMeter.count();
    }

    public long getTransportErrorCount() {
        return this.transportErrorMeter.count();
    }

    @Override
    public long getBadRequestCount() {
        return this.badRequestMeter.count();
    }

    public long getNumBusyConnections() {
        return this.connectionBorrowMeter.count() - this.connectionReturnMeter.count();
    }

    public long getNumOpenConnections() {
        return this.connectionCreateMeter.count() - this.connectionClosedMeter.count();
    }

    @Override
    public long notFoundCount() {
        return this.notFoundMeter.count();
    }

    @Override
    public long getHostCount() {
        return getHostAddedCount() - getHostRemovedCount();
    }

    public long getHostActiveCount() {
        return hostAddedMeter.count() - hostRemovedMeter.count() + hostReactivatedMeter.count() - hostDownMeter.count();
    }

    public String toString() {
        // Build the complete status string
        return new StringBuilder()
                .append("InstrumentedConnectionPoolMonitor(")
                .append("Connections[" )
                .append( "open="       ).append(getNumOpenConnections())
                .append(",busy="       ).append(getNumBusyConnections())
                .append(",create="     ).append(connectionCreateMeter.count())
                .append(",close="      ).append(connectionClosedMeter.count())
                .append(",failed="     ).append(connectionCreateFailureMeter.count())
                .append(",borrow="     ).append(connectionBorrowMeter.count())
                .append(",return=").append(connectionReturnMeter.count())
                .append("], Operations[")
                .append("success=").append(operationSuccessMeter.count())
                .append(",failure=").append(operationFailureMeter.count())
                .append(",optimeout=").append(operationTimeoutMeter.count())
                .append(",timeout=").append(socketTimeoutMeter.count())
                .append(",failover=").append(operationFailoverMeter.count())
                .append(",nohosts=").append(noHostsMeter.count())
                .append(",unknown=").append(unknownErrorMeter.count())
                .append(",interrupted=").append(interruptedMeter.count())
                .append(",exhausted="  ).append(poolExhaustedMeter.count())
                .append(",transport="  ).append(transportErrorMeter.count())
                .append("], Hosts[")
                .append("add=").append(hostAddedMeter.count())
                .append(",remove=").append(hostRemovedMeter.count())
                .append(",down=").append(hostDownMeter.count())
                .append(",reactivate=").append(hostReactivatedMeter.count())
                .append(",active=").append(getHostActiveCount())
                .append("])").toString();
    }

    @Override
    public Map<Host, HostStats> getHostStats() {
        throw new UnsupportedOperationException("Not supported");
    }
}
