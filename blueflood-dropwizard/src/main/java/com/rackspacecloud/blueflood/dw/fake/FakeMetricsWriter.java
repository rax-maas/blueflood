package com.rackspacecloud.blueflood.dw.fake;

import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

// no ops for now.

public class FakeMetricsWriter implements IMetricsWriter {
    private static final Logger log = LoggerFactory.getLogger(FakeMetricsWriter.class);    
    
    public FakeMetricsWriter() {
    }

    @Override
    public void insertFullMetrics(Collection<Metric> metrics) throws IOException {
        log.debug("Will write {} full res metrics", metrics.size());
    }

    @Override
    public void insertPreaggreatedMetrics(Collection<IMetric> metrics) throws IOException {
        log.debug("Will write {} preaggregated metrics", metrics.size());
    }
}
