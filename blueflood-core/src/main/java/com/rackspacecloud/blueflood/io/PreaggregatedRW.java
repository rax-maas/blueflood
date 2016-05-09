package com.rackspacecloud.blueflood.io;

import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.IMetric;

import java.io.IOException;
import java.util.Collection;

/**
 * Created by rona6028 on 5/4/16.
 */
public interface PreaggregatedRW {


    /**
     * Inserts a collection of rolled up metrics to the metrics_preaggregated_{granularity} column family.
     * Only our tests should call this method. Services should call either insertMetrics(Collection metrics)
     * or insertRollups()
     *
     * @param metrics
     * @throws IOException
     */
    @VisibleForTesting
    public abstract void insertMetrics(Collection<IMetric> metrics, Granularity granularity) throws IOException;
}
