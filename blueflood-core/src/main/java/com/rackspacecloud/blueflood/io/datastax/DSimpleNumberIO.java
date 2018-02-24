package com.rackspacecloud.blueflood.io.datastax;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.rackspacecloud.blueflood.io.serializers.metrics.RawSerDes;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.IMetric;

import java.nio.ByteBuffer;

/**
 * This class holds the utility methods to read/write simple numbers
 * using Datastax driver. This is mostly used during Ingest and metrics
 * are inserted to the metrics_full Column Family
 */
public class DSimpleNumberIO extends DAbstractMetricIO {

    private RawSerDes serDes = new RawSerDes();

    @Override
    protected ByteBuffer toByteBuffer( Object value ) {

        return serDes.serialize( value );
    }

    @Override
    protected Object fromByteBuffer( ByteBuffer byteBuffer ) {

        return serDes.deserialize( byteBuffer );
    }

    /**
     * Inserts a raw metric (not its rolled up value) to the proper
     * column family
     *
     * @param metric
     * @return
     */
    public ResultSetFuture insertRawAsync( IMetric metric ) {

        BoundStatement bound = getBoundStatementForMetric(metric);
        return session.executeAsync(bound);

    }

    /**
     * Constructs a {@link BoundStatement} for a particular metric.
     * The statement could be part of a BatchStatement or it could
     * be executed as is.
     *
     * @param metric
     * @return
     */
    public BoundStatement getBoundStatementForMetric(IMetric metric) {
        return metricsCFPreparedStatements.insertToMetricsBasicFullStatement.bind(
                metric.getLocator().toString(),
                metric.getCollectionTime(),
                serDes.serialize( metric.getMetricValue() ),
                metric.getTtlInSeconds() );
    }

    @Override
    protected BoundStatement getBoundStatementForMetric(IMetric metric, Granularity granularity) {
        // we ignore granularity from the abstract parent because
        // this class is only used to write to metrics_full
        return getBoundStatementForMetric(metric);
    }
}
