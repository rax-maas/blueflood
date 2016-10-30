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

package com.rackspacecloud.blueflood.io.astyanax;

import com.codahale.metrics.Timer;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.cache.CombinedTtlProvider;
import com.rackspacecloud.blueflood.cache.LocatorCache;
import com.rackspacecloud.blueflood.cache.TenantTtlProvider;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.io.serializers.Serializers;
import com.rackspacecloud.blueflood.io.serializers.astyanax.StringMetadataSerializer;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AstyanaxWriter extends AstyanaxIO {
    private static final Logger log = LoggerFactory.getLogger(AstyanaxWriter.class);
    private static final AstyanaxWriter instance = new AstyanaxWriter();
    private static final Keyspace keyspace = getKeyspace();

    private boolean areStringMetricsDropped = Configuration.getInstance().getBooleanProperty(CoreConfig.STRING_METRICS_DROPPED);
    private List<String> tenantIdsKept = Configuration.getInstance().getListProperty(CoreConfig.TENANTIDS_TO_KEEP);
    private Set<String> keptTenantIdsSet = new HashSet<String>(tenantIdsKept);

    public static AstyanaxWriter getInstance() {
        return instance;
    }

    private static TenantTtlProvider TTL_PROVIDER = CombinedTtlProvider.getInstance();

    private static Granularity DELAYED_METRICS_STORAGE_GRANULARITY =
            Granularity.getRollupGranularity(Configuration.getInstance().getStringProperty(CoreConfig.DELAYED_METRICS_STORAGE_GRANULARITY));

    private static final long MAX_AGE_ALLOWED = Configuration.getInstance().getLongProperty(CoreConfig.ROLLUP_DELAY_MILLIS);

    private boolean shouldPersistStringMetric(IMetric metric) {
        String tenantId = metric.getLocator().getTenantId();

        if(areStringMetricsDropped && !keptTenantIdsSet.contains(tenantId) ) {
            return false;
        }
        else {
            String currentValue = String.valueOf(metric.getMetricValue());
            final String lastValue = AstyanaxReader.getInstance().getLastStringValue(metric.getLocator());

            return lastValue == null || !currentValue.equals(lastValue);
        }
    }

    private boolean shouldPersist(IMetric metric) {
        boolean shouldPersistMetric = true;
        try {
            final DataType metricType = DataType.getMetricType( metric.getMetricValue() );
            if (metricType.equals(DataType.STRING) || metricType.equals(DataType.BOOLEAN)) {
                shouldPersistMetric = shouldPersistStringMetric(metric);
            }
        } catch (Exception e) {
            // If we hit any exception, just persist the metric
            shouldPersistMetric = true;
        }

        return shouldPersistMetric;
    }

    // insert a full resolution chunk of data. I've assumed that there will not be a lot of overlap (these will all be
    // single column updates).
    public void insertFull(Collection<? extends IMetric> metrics, boolean isRecordingDelayedMetrics, Clock clock) throws ConnectionException {
        Timer.Context ctx = Instrumentation.getWriteTimerContext(CassandraModel.CF_METRICS_FULL_NAME);

        try {
            MutationBatch mutationBatch = keyspace.prepareMutationBatch();
            for (IMetric metric: metrics) {
                final Locator locator = metric.getLocator();

                final boolean isString = DataType.isStringMetric( metric.getMetricValue() );
                final boolean isBoolean = DataType.isBooleanMetric( metric.getMetricValue() );

                if (!shouldPersist(metric)) {
                    log.trace("Metric shouldn't be persisted, skipping insert", metric.getLocator().toString());
                    continue;
                }

                // key = shard
                // col = locator (acct + entity + check + dimension.metric)
                // value = <nothing>
                // do not do it for string or boolean metrics though.
                if (!LocatorCache.getInstance().isLocatorCurrent(locator)) {
                    if (!isString && !isBoolean && mutationBatch != null)
                        insertLocator(locator, mutationBatch);
                    LocatorCache.getInstance().setLocatorCurrent(locator);
                }

                if (isRecordingDelayedMetrics) {
                    //retaining the same conditional logic that was used to insertLocator(locator, batch) above.
                    if (!isString && !isBoolean && mutationBatch != null) {
                        insertLocatorIfDelayed(metric, mutationBatch, clock);
                    }
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

    /**
     * This method inserts the locator into the metric_delayed_locator column family, if the metric is delayed.
     *
     * @param metric
     * @param mutationBatch
     * @param clock
     */
    private void insertLocatorIfDelayed(IMetric metric, MutationBatch mutationBatch, Clock clock) {
        Locator locator = metric.getLocator();

        long delay = clock.now().getMillis() - metric.getCollectionTime();
        if (delay > MAX_AGE_ALLOWED) {

            //track locator for configured granularity level. to re-roll only the delayed locator's for that slot
            int slot = DELAYED_METRICS_STORAGE_GRANULARITY.slot(metric.getCollectionTime());
            if (!LocatorCache.getInstance().isDelayedLocatorForASlotCurrent(slot, locator)) {
                insertDelayedLocator(DELAYED_METRICS_STORAGE_GRANULARITY, slot, locator, mutationBatch);
                LocatorCache.getInstance().setDelayedLocatorForASlotCurrent(slot, locator);
            }
        }
    }


    // numeric only!
    public final void insertLocator(Locator locator, MutationBatch mutationBatch) {
        mutationBatch.withRow(CassandraModel.CF_METRICS_LOCATOR, (long) Util.getShard(locator.toString()))
                .putEmptyColumn(locator, TenantTtlProvider.LOCATOR_TTL);
    }

    // numeric only!
    public final void insertDelayedLocator(Granularity g, int slot, Locator locator, MutationBatch mutationBatch) {
        int shard = Util.getShard(locator.toString());
        mutationBatch.withRow(CassandraModel.CF_METRICS_DELAYED_LOCATOR, SlotKey.of(g, slot, shard))
                .putEmptyColumn(locator, TenantTtlProvider.DELAYED_LOCATOR_TTL);
    }

    private final void insertEnumValuesWithHashcodes(Locator locator, BluefloodEnumRollup rollup, MutationBatch mutationBatch) {
        for(String valueName : rollup.getStringEnumValuesWithCounts().keySet()) {
            mutationBatch.withRow(CassandraModel.CF_METRICS_ENUM, locator).putColumn((long)valueName.hashCode(), valueName);
            Instrumentation.markEnumMetricWritten();
        }
    }

    private void insertMetric(IMetric metric, MutationBatch mutationBatch) {
        final boolean isString = DataType.isStringMetric(metric.getMetricValue());
        final boolean isBoolean = DataType.isBooleanMetric(metric.getMetricValue());

        if (isString || isBoolean) {
            // they were already casting long to int in Metrics.setTtl()
            metric.setTtlInSeconds((int)TTL_PROVIDER.getTTLForStrings(metric.getLocator().getTenantId()).get().toSeconds());
            String persist;
            if (isString) {
                persist = (String) metric.getMetricValue();
            } else { //boolean
                persist = String.valueOf(metric.getMetricValue());
            }
            mutationBatch.withRow(CassandraModel.CF_METRICS_STRING, metric.getLocator())
                    .putColumn(metric.getCollectionTime(), persist, metric.getTtlInSeconds());
        } else {
            try {
                mutationBatch.withRow(CassandraModel.CF_METRICS_FULL, metric.getLocator())
                        .putColumn(metric.getCollectionTime(),
                                metric.getMetricValue(),
                                Serializers.serializerFor(Object.class),
                                metric.getTtlInSeconds());
            } catch (RuntimeException e) {
                log.error("Error serializing full resolution data", e);
            }
        }
    }

    public void writeMetadataValue(Locator locator, String metaKey, String metaValue) throws ConnectionException {
        Timer.Context ctx = Instrumentation.getWriteTimerContext(CassandraModel.CF_METRICS_METADATA_NAME);
        try {
            keyspace.prepareColumnMutation(CassandraModel.CF_METRICS_METADATA, locator, metaKey)
                    .putValue(metaValue, StringMetadataSerializer.get(), null)
                    .execute();
        } catch (ConnectionException e) {
            Instrumentation.markWriteError(e);
            log.error("Error writing Metadata Value", e);
            throw e;
        } finally {
            ctx.stop();
        }
    }

    public void writeMetadata(Table<Locator, String, String> metaTable) throws ConnectionException {
        ColumnFamily cf = CassandraModel.CF_METRICS_METADATA;
        Timer.Context ctx = Instrumentation.getBatchWriteTimerContext(CassandraModel.CF_METRICS_METADATA_NAME);
        MutationBatch batch = keyspace.prepareMutationBatch();

        try {
            for (Locator locator : metaTable.rowKeySet()) {
                Map<String, String> metaRow = metaTable.row(locator);
                ColumnListMutation<String> mutation = batch.withRow(cf, locator);

                for (Map.Entry<String, String> meta : metaRow.entrySet()) {
                    mutation.putColumn(meta.getKey(), meta.getValue(), StringMetadataSerializer.get(), null);
                }
            }
            try {
                batch.execute();
            } catch (ConnectionException e) {
                Instrumentation.markWriteError(e);
                log.error("Connection exception persisting metadata", e);
                throw e;
            }
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
    public void insertMetrics(Collection<IMetric> metrics, ColumnFamily cf, boolean isRecordingDelayedMetrics, Clock clock) throws ConnectionException {
        Timer.Context ctx = Instrumentation.getWriteTimerContext(cf.getName());
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
                        final boolean isString = DataType.isStringMetric(metric.getMetricValue());
                        final boolean isBoolean = DataType.isBooleanMetric(metric.getMetricValue());

                        if (!isString && !isBoolean)
                            locatorInsertOk = true;
                        shouldPersist = shouldPersist((Metric)metric);
                    } else {
                        if (DataType.isEnumMetric(metric.getMetricValue())) {
                            insertEnumValuesWithHashcodes(metric.getLocator(), (BluefloodEnumRollup) metric.getMetricValue(), batch);
                        }
                        locatorInsertOk = true;
                    }
                    
                    if (shouldPersist) {
                        mutation.putColumn(
                                metric.getCollectionTime(),
                                metric.getMetricValue(),
                                (AbstractSerializer) (Serializers.serializerFor(metric.getMetricValue().getClass())),
                                metric.getTtlInSeconds());
                        if (cf.getName().equals(CassandraModel.CF_METRICS_PREAGGREGATED_FULL_NAME)) {
                            Instrumentation.markFullResPreaggregatedMetricWritten();
                        }
                    }

                    if (isRecordingDelayedMetrics) {
                        //retaining the same conditional logic that was used to perform insertLocator(locator, batch).
                        if (locatorInsertOk) {
                            insertLocatorIfDelayed(metric, batch, clock);
                        }
                    }
                }
                
                if (!LocatorCache.getInstance().isLocatorCurrent(locator)) {
                    if (locatorInsertOk)
                        insertLocator(locator, batch);
                    LocatorCache.getInstance().setLocatorCurrent(locator);
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

    public void insertRollups(List<SingleRollupWriteContext> writeContexts) throws ConnectionException {
        if (writeContexts.size() == 0) {
            return;
        }
        Timer.Context ctx = Instrumentation.getBatchWriteTimerContext(writeContexts.get(0).getDestinationCF().getName());
        MutationBatch mb = keyspace.prepareMutationBatch();
        for (SingleRollupWriteContext writeContext : writeContexts) {
            Rollup rollup = writeContext.getRollup();
            int ttl = (int)TTL_PROVIDER.getTTL(
                    writeContext.getLocator().getTenantId(),
                    writeContext.getGranularity(),
                    writeContext.getRollup().getRollupType()).get().toSeconds();
            AbstractSerializer serializer = Serializers.serializerFor(rollup.getClass());
            try {
                mb.withRow(writeContext.getDestinationCF(), writeContext.getLocator())
                        .putColumn(writeContext.getTimestamp(),
                                rollup,
                                serializer,
                                ttl);
            } catch (RuntimeException ex) {
                // let's not let stupidness prevent the rest of this put.
                log.warn(String.format("Cannot save %s", writeContext.getLocator().toString()), ex);
            }
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
