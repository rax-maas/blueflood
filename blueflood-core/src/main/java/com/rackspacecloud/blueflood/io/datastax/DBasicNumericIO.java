package com.rackspacecloud.blueflood.io.datastax;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.io.serializers.metrics.BasicRollupSerDes;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


/**
 * This class holds the utility methods to read/write rolled up basic numeric values in
 * {@link com.rackspacecloud.blueflood.types.BasicRollup} using {@link BasicRollupSerDes}
 */
public class DBasicNumericIO extends DAbstractMetricIO {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(DBasicNumericIO.class);
    private BasicRollupSerDes serDes = new BasicRollupSerDes();

    @Override
    protected ByteBuffer toByteBuffer( Object value ) {

        if ( ! (value instanceof BasicRollup ) ) {
            throw new IllegalArgumentException("toByteBuffer(): expecting BasicRollup class but got " + value.getClass().getSimpleName());
        }

        return serDes.serialize( (BasicRollup)value );
    }

    @Override
    protected Object fromByteBuffer( ByteBuffer byteBuffer ) {

        return serDes.deserialize( byteBuffer );
    }

    /**
     *
     * @param metric
     * @param granularity
     * @return
     */
    @Override
    protected BoundStatement getBoundStatementForMetric(IMetric metric, Granularity granularity) {
        Object metricValue = metric.getMetricValue();
        if ( ! (metricValue instanceof BasicRollup) ) {
            throw new InvalidDataException(
                    String.format("getBoundStatementForMetric(locator=%s, granularity=%s): metric value %s is not type BasicRollup",
                            metric.getLocator(), granularity, metric.getMetricValue().getClass().getSimpleName())
            );
        }
        PreparedStatement statement = metricsCFPreparedStatements.basicGranToInsertStatement.get(granularity);
        return statement.bind(
                metric.getLocator().toString(),
                metric.getCollectionTime(),
                serDes.serialize( (BasicRollup) metric.getMetricValue() ),
                metric.getTtlInSeconds() );

    }
}
