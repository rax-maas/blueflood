package com.rackspacecloud.blueflood.io.datastax;


import com.rackspacecloud.blueflood.io.serializers.metrics.BasicRollupSerDes;
import com.rackspacecloud.blueflood.types.*;

import java.nio.ByteBuffer;


/**
 * This class holds the utility methods to read/write basic numeric metrics
 * using Datastax driver.
 */
public class DBasicNumericIO extends DAbstractMetricIO {

    private BasicRollupSerDes serDes = new BasicRollupSerDes();

    @Override
    protected ByteBuffer toByteBuffer( Object rollup ) {

        if ( ! (rollup instanceof BasicRollup ) ) {
            throw new IllegalArgumentException("toByteBuffer(): expecting BasicRollup class but got " + rollup.getClass().getSimpleName());
        }

        return serDes.serialize( (BasicRollup)rollup );
    }

    @Override
    protected Object fromByteBuffer( ByteBuffer byteBuffer ) {

        return serDes.deserialize( byteBuffer );
    }
}
