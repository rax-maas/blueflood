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

import com.codahale.metrics.Timer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.cache.TtlCache;
import com.rackspacecloud.blueflood.internal.Account;
import com.rackspacecloud.blueflood.internal.InternalAPIFactory;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.MetricsPersistenceOptimizer;
import com.rackspacecloud.blueflood.rollup.MetricsPersistenceOptimizerFactory;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.service.UpdateStamp;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class AstyanaxWriter extends AstyanaxIO {
    private static final Logger log = LoggerFactory.getLogger(AstyanaxWriter.class);
    private static final AstyanaxWriter instance = new AstyanaxWriter();
    private static final Keyspace keyspace = getKeyspace();
    private static final int CACHE_CONCURRENCY = config.getIntegerProperty(CoreConfig.MAX_ROLLUP_WRITE_THREADS);

    private static final TimeValue STRING_TTL = new TimeValue(730, TimeUnit.DAYS); // 2 years
    private static final int LOCATOR_TTL = 604800;  // in seconds (7 days)

    private static final String INSERT_ROLLUP_BATCH = "Rollup Batch Insert".intern();

    public static AstyanaxWriter getInstance() {
        return instance;
    }

    private static TtlCache ROLLUP_TTL_CACHE = new TtlCache(
            "Rollup TTL Cache",
            new TimeValue(120, TimeUnit.HOURS),
            CACHE_CONCURRENCY,
            InternalAPIFactory.createDefaultTTLProvider()) {
        // we do not care about caching full res values.
        @Override
        protected Map<ColumnFamily<Locator, Long>, TimeValue> buildTtlMap(Account acct) {
            Map<ColumnFamily<Locator, Long>, TimeValue> map = super.buildTtlMap(acct);
            map.remove("full");
            return map;
        }
    };

    // this collection is used to reduce the number of locators that get written.  Simply, if a locator has been
    // written in the last 10 minutes, don't bother.
    private static final Cache<String, Boolean> insertedLocators = CacheBuilder.newBuilder().expireAfterWrite(10,
            TimeUnit.MINUTES).concurrencyLevel(16).build();


    private boolean shouldPersist(Metric metric) {
        try {
            final Metric.Type metricType = metric.getType();
            final MetricsPersistenceOptimizer optimizer =
                    MetricsPersistenceOptimizerFactory.getOptimizer(metricType);

            return optimizer.shouldPersist(metric);
        } catch (Exception e) {
            // If we hit any exception, just persist the metric
            return true;
        }
    }

    // insert a full resolution chunk of data. I've assumed that there will not be a lot of overlap (these will all be
    // single column updates).
    public void insertFull(List<Metric> metrics) throws ConnectionException {
        Timer.Context ctx = Instrumentation.getWriteTimerContext(AstyanaxIO.CF_METRICS_FULL);

        try {
            MutationBatch mutationBatch = keyspace.prepareMutationBatch();
            for (Metric metric: metrics) {
                final Locator locator = metric.getLocator();

                final boolean isString = metric.isString();
                final boolean isBoolean = metric.isBoolean();

                if (!shouldPersist(metric)) {
                    log.debug("Metric shouldn't be persisted, skipping insert", metric.getLocator().toString());
                    continue;
                }

                // key = shard
                // col = locator (acct + entity + check + dimension.metric)
                // value = <nothing>
                // do not do it for string or boolean metrics though.
                if (!AstyanaxWriter.isLocatorCurrent(locator)) {
                    if (!isString && !isBoolean && mutationBatch != null)
                        insertLocator(locator, mutationBatch);
                    AstyanaxWriter.setLocatorCurrent(locator);
                }

                insertMetric(metric, mutationBatch);
                Instrumentation.markFullResMetricWritten();
            }
            // insert it
            try {
                mutationBatch.execute();
            } catch (ConnectionException e) {
                Instrumentation.markWriteError(e);
                log.error("Connection exception during insertFull", e);
                throw e;
            }
        } finally {
            ctx.stop();
        }
    }

    // numeric only!
    private final void insertLocator(Locator locator, MutationBatch mutationBatch) {
        mutationBatch.withRow(CF_METRICS_LOCATOR, (long) Util.computeShard(locator.toString()))
                .putEmptyColumn(locator, LOCATOR_TTL);
    }

    private void insertMetric(Metric metric, MutationBatch mutationBatch) {
        final boolean isString = metric.isString();
        final boolean isBoolean = metric.isBoolean();

        if (isString || isBoolean) {
            metric.setTtl(STRING_TTL);
            String persist;
            if (isString) {
                persist = (String) metric.getValue();
            } else { //boolean
                persist = String.valueOf(metric.getValue());
            }
            mutationBatch.withRow(CF_METRICS_STRING, metric.getLocator())
                    .putColumn(metric.getCollectionTime(), persist, metric.getTtlInSeconds());
        } else {
            try {
                mutationBatch.withRow(CF_METRICS_FULL, metric.getLocator())
                        .putColumn(metric.getCollectionTime(),
                                metric.getValue(),
                                NumericSerializer.serializerFor(Object.class),
                                metric.getTtlInSeconds());
            } catch (RuntimeException e) {
                log.error("Error serializing full resolution data", e);
            }
        }
    }

    public void writeMetadataValue(Locator locator, String metaKey, Object metaValue) throws ConnectionException {
        Timer.Context ctx = Instrumentation.getWriteTimerContext(CF_METRIC_METADATA);
        try {
            keyspace.prepareColumnMutation(CF_METRIC_METADATA, locator, metaKey)
                    .putValue(metaValue, MetadataSerializer.get(), null)
                    .execute();
        } catch (ConnectionException e) {
            Instrumentation.markWriteError(e);
            log.error("Error writing Metadata Value", e);
            throw e;
        } finally {
            ctx.stop();
        }
    }
    
    private static Multimap<Locator, IMetric> asMultimap(Collection<IMetric> metrics) {
        Multimap<Locator, IMetric> map = LinkedListMultimap.create();
        for (IMetric metric: metrics)
            map.put(metric.getLocator(), metric);
        return map;
    }
    
    // generic IMetric insertion. All other metric insertion methods could use this one.
    public void insertMetrics(Collection<IMetric> metrics, ColumnFamily cf) throws ConnectionException {
        // todo: need a way of using an interned string.
        Timer.Context ctx = Instrumentation.getWriteTimerContext(cf);
        Multimap<Locator, IMetric> map = asMultimap(metrics);
        MutationBatch batch = keyspace.prepareMutationBatch();
        try {
            for (Locator locator : map.keySet()) {
                ColumnListMutation<Long> mutation = batch.withRow(cf, locator);
                
                // we want to insert a locator only for non-string, non-boolean metrics. If there happen to be string or
                // boolean metrics mixed in with numeric metrics, we still want to insert a locator.  If all metrics
                // are boolean or string, we DO NOT want to insert a locator.
                boolean locatorInsertOk = false;
                
                for (IMetric metric : map.get(locator)) {
                    
                    boolean shouldPersist = true;
                    // todo: MetricsPersistenceOptimizerFactory interface needs to be retooled to accept IMetric
                    if (metric instanceof Metric) {
                        final boolean isString = Metric.Type.isStringMetric(metric.getValue());
                        final boolean isBoolean = Metric.Type.isBooleanMetric(metric.getValue());
                        
                        
                        if (!isString && !isBoolean)
                            locatorInsertOk = true;
                        shouldPersist = shouldPersist((Metric)metric);
                    } else {
                        locatorInsertOk = true;
                    }
                    
                    if (shouldPersist) {
                        mutation.putColumn(
                                metric.getCollectionTime(),
                                metric.getValue(),
                                (AbstractSerializer) (NumericSerializer.serializerFor(metric.getValue().getClass())),
                                metric.getTtlInSeconds());
                    }
                }
                
                if (!AstyanaxWriter.isLocatorCurrent(locator)) {
                    if (locatorInsertOk)
                        insertLocator(locator, batch);
                    AstyanaxWriter.setLocatorCurrent(locator);
                }
            }
            try {
                batch.execute();
            } catch (ConnectionException e) {
                Instrumentation.markWriteError(e);
                log.error("Connection exception persisting data", e);
                throw e;
            }
        } finally {
            ctx.stop();
        }
    }

    public void persistShardState(int shard, Map<Granularity, Map<Integer, UpdateStamp>> updates) throws ConnectionException {
        Timer.Context ctx = Instrumentation.getWriteTimerContext(CF_METRICS_STATE);
        try {
            boolean presenceSentinel = false;
            MutationBatch mutationBatch = keyspace.prepareMutationBatch();
            ColumnListMutation<String> mutation = mutationBatch.withRow(CF_METRICS_STATE, (long)shard);
            for (Map.Entry<Granularity, Map<Integer, UpdateStamp>> granEntry : updates.entrySet()) {
                Granularity g = granEntry.getKey();
                for (Map.Entry<Integer, UpdateStamp> entry : granEntry.getValue().entrySet()) {
                    // granularity,slot,state
                    String columnName = Util.formatStateColumnName(g, entry.getKey(), entry.getValue().getState().code());
                    mutation.putColumn(columnName, entry.getValue().getTimestamp())
                                    // notice the sleight-of-hand here. The column timestamp is getting set to be the timestamp that is being
                                    // written. this effectively creates a check-then-set update that fails if the value currently in the
                                    // database is newer.
                                    // multiply by 1000 to produce microseconds from milliseconds.
                            .setTimestamp(entry.getValue().getTimestamp() * 1000);
                    presenceSentinel = true;
                }
            }
            if (presenceSentinel)
                try {
                    mutationBatch.execute();
                } catch (ConnectionException e) {
                    Instrumentation.markWriteError(e);
                    log.error("Error persisting shard state", e);
                    throw e;
                }
        } finally {
            ctx.stop();
        }
    }

    protected static boolean isLocatorCurrent(Locator loc) {
        return insertedLocators.getIfPresent(loc) != null;
    }

    private static void setLocatorCurrent(Locator loc) {
        insertedLocators.put(loc.toString(), Boolean.TRUE);
    }

    public void insertRollups(ArrayList<SingleRollupWriteContext> writeContexts) throws ConnectionException {
        Timer.Context ctx = Instrumentation.getWriteTimerContext(INSERT_ROLLUP_BATCH);
        MutationBatch mb = keyspace.prepareMutationBatch();
        for (SingleRollupWriteContext writeContext : writeContexts) {
            Rollup rollup = writeContext.getRollup();
            int ttl = (int) ROLLUP_TTL_CACHE.getTtl(
                    writeContext.getLocator().getTenantId(),
                    writeContext.getDestinationCF()).toSeconds();
            AbstractSerializer serializer = NumericSerializer.serializerFor(rollup.getClass());
            mb.withRow(writeContext.getDestinationCF(), writeContext.getLocator())
                    .putColumn(writeContext.getTimestamp(),
                            rollup,
                            serializer,
                            ttl);
        }
        try {
            mb.execute();
        } catch (ConnectionException e) {
            Instrumentation.markWriteError(e);
            log.error("Error writing rollup batch", e);
            throw e;
        } finally {
            ctx.stop();
        }
    }
}
