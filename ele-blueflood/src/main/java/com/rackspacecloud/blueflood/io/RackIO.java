package com.rackspacecloud.blueflood.io;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.shallows.EmptyColumnList;
import com.rackspacecloud.blueflood.cm.Util;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.ServerMetricLocator;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RackIO extends AstyanaxIO {
    // intentional: use the same logger as AstyanaxReader.
    private static final Logger log = LoggerFactory.getLogger(AstyanaxReader.class);
    
    private static final String GET_METRICS_FOR_CHECK = "Get list of metrics for check";
    private static final int METRICS_DISCOVERY_TTL = 31536000; // in seconds (365 days)
    
    protected static final ColumnFamily<String, String> CF_METRICS_DISCOVERY = new ColumnFamily<String, String>("metrics_discovery",
            StringSerializer.get(),
            StringSerializer.get());
    
    private static final Keyspace keyspace = getKeyspace();
    private static final RackIO INSTANCE = new RackIO();
    
    public static RackIO getInstance() {
        return INSTANCE;
    }
    
    public ColumnList<String> getMetricsList(final String dBKey) {
        TimerContext ctx = Instrumentation.getTimerContext(GET_METRICS_FOR_CHECK);
        try {
            RowQuery<String, String> query = keyspace
                    .prepareQuery(CF_METRICS_DISCOVERY)
                    .getKey(dBKey);
            return query.execute().getResult();
        } catch (NotFoundException e) {
            return new EmptyColumnList<String>();
        } catch (ConnectionException e) {
            Instrumentation.markReadError(e);
            log.error("Error getting metrics list", e);
            throw new RuntimeException("Error getting metrics list", e);
        } finally {
            ctx.stop();
        }
    }
    
    public void insertDiscovery(List<Metric> metrics) throws ConnectionException {
        // todo: needs a metric.
        MutationBatch mutationBatch = keyspace.prepareMutationBatch();

        boolean hasDiscovery = false;
        for (Metric metric: metrics) {
            final Locator locator = metric.getLocator();
            if (!AstyanaxWriter.isLocatorCurrent(locator)) {
                hasDiscovery = true;
                insertDiscovery(locator, mutationBatch);
            }
        }
        
        if (hasDiscovery) {
            try {
                mutationBatch.execute();
            } catch (ConnectionException ex) {
                Instrumentation.markWriteError(ex);
                log.error("Connection exception during insertDiscovery");
                throw ex;
            }
        }
    }
    
    private void insertDiscovery(Locator locator, MutationBatch mutationBatch) {
        if (locator instanceof ServerMetricLocator) {
            ServerMetricLocator serverMetricLocator = (ServerMetricLocator) locator;
            String rowKey = Util.generateMetricsDiscoveryDBKey(serverMetricLocator.getAccountId(),
                    serverMetricLocator.getEntityId(), serverMetricLocator.getCheckId());
            String colKey = serverMetricLocator.getMetric();
            mutationBatch.withRow(CF_METRICS_DISCOVERY, rowKey)
                    .putColumn(colKey, "", METRICS_DISCOVERY_TTL);
        }
    }
}
