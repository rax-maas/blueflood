package com.rackspacecloud.blueflood.io.datastax;


import com.rackspacecloud.blueflood.io.serializers.metrics.BasicRollupSerDes;
import com.rackspacecloud.blueflood.types.*;

import java.nio.ByteBuffer;


/**
 * This class holds the utility methods to read/write rolled up basic numeric values using
 * {@link com.rackspacecloud.blueflood.types.BasicRollup}.
 */
public class DBasicNumericIO extends DAbstractMetricIO {

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
}
