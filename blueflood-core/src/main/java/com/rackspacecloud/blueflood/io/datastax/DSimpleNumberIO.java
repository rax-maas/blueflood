package com.rackspacecloud.blueflood.io.datastax;

import com.rackspacecloud.blueflood.io.serializers.metrics.RawSerDes;
import com.rackspacecloud.blueflood.types.Rollup;

import java.nio.ByteBuffer;

/**
 * This class holds the utility methods to read/write simple numbers
 * using Datastax driver.
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
}
