package com.cloudkick.blueflood.rollup;

/**
 * The intent of the interface is to provide standard API calls to users
 * that want to optimize metrics persistence. One typical optimization is
 * to query a database and check if the previous entry for the metric has
 * the same value as the current one. This is useful for string and state
 * metrics where we do not want to persist the same metric value multiple
 * times
 */
public interface MetricsPersistenceOptimizer {
    boolean shouldPersist(com.cloudkick.blueflood.types.Metric metric) throws Exception;
}