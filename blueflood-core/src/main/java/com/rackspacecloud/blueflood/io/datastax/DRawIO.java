package com.rackspacecloud.blueflood.io.datastax;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.serializers.metrics.RawSerDes;
import com.rackspacecloud.blueflood.types.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * This class deals with writing boolean, numeric and string values to the metrics_full column
 * families using the Datastax driver.
 *
 * This class does not subclass the {@link com.rackspacecloud.blueflood.io.datastax.DAbstractMetricIO} as serializes
 * its values as a String and not ByteBuffer like the rest of the IO objects.
 *
 */
public class DRawIO {

    private static final Logger LOG = LoggerFactory.getLogger( DRawIO.class );

    public static final String KEY = "key";
    public static final String COLUMN1 = "column1";
    public static final String VALUE = "value";

    private final PreparedStatement putNumeric;

    private RawSerDes serDes = new RawSerDes();

    public DRawIO() {

        Session session = DatastaxIO.getSession();

        Insert.Options insertNumeric = insertInto( CassandraModel.CF_METRICS_FULL_NAME )
                .value( KEY , bindMarker() )
                .value( COLUMN1, bindMarker() )
                .value( VALUE, bindMarker() )
                .using( ttl( bindMarker() ) );

        putNumeric = session.prepare( insertNumeric );
        putNumeric.setConsistencyLevel( ConsistencyLevel.LOCAL_ONE );
    }

    public ResultSetFuture insertAsync( IMetric metric ) {

        BoundStatement bound = putNumeric.bind( metric.getLocator().toString(),
                    metric.getCollectionTime(),
                    serDes.serialize( metric.getMetricValue() ),
                    metric.getTtlInSeconds() );

        return DatastaxIO.getSession().executeAsync( bound );
    }
}
