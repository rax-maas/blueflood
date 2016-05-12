package com.rackspacecloud.blueflood.io.datastax;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.io.serializers.metrics.RawSerDes;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.types.DataType;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;


/**
 * This class deals with writing boolean, numeric and string values to the metrics_full & metrics_string column
 * families using the Datastax driver.
 *
 * This class does not subclass the {@link com.rackspacecloud.blueflood.io.datastax.DAbstractMetricIO} as serializes
 * its values as a String and not ByteBuffer like the rest of the IO objects.
 *
 */
public class DRawIO {

    private static final Logger LOG = LoggerFactory.getLogger( DRawIO.class );

    private static final TimeValue STRING_TTL = new TimeValue(730, TimeUnit.DAYS); // 2 years
    public static final String KEY = "key";
    public static final String COLUMN1 = "column1";
    public static final String VALUE = "value";

    private final PreparedStatement putString;
    private final PreparedStatement putNumeric;
    private final PreparedStatement getString;
    private final PreparedStatement getLastString;

    private RawSerDes serDes = new RawSerDes();

    public DRawIO() {

        Session session = DatastaxIO.getSession();

        Insert.Options insertString = insertInto( CassandraModel.CF_METRICS_STRING_NAME )
                .value( KEY , bindMarker() )
                .value( COLUMN1, bindMarker() )
                .value( VALUE, bindMarker() )
                .using( ttl( bindMarker() ) );

        putString = session.prepare( insertString );

        // TODO: This is required by the cassandra-maven-plugin 2.0.0-1, but not by cassandra 2.0.11, which we run.
        // I believe its due to the bug https://issues.apache.org/jira/browse/CASSANDRA-6238
        putString.setConsistencyLevel( ConsistencyLevel.ONE );

        Insert.Options insertNumeric = insertInto( CassandraModel.CF_METRICS_FULL_NAME )
                .value( KEY , bindMarker() )
                .value( COLUMN1, bindMarker() )
                .value( VALUE, bindMarker() )
                .using( ttl( bindMarker() ) );

        putNumeric = session.prepare( insertNumeric );

        // TODO: This is required by the cassandra-maven-plugin 2.0.0-1, but not by cassandra 2.0.11, which we run.
        // I believe its due to the bug https://issues.apache.org/jira/browse/CASSANDRA-6238
        putNumeric.setConsistencyLevel( ConsistencyLevel.ONE );


        Select.Where whereString = select()
                .all()
                .from( CassandraModel.CF_METRICS_STRING_NAME )
                .where( eq( KEY, bindMarker() ) )
                .and(  gte( COLUMN1, bindMarker() ) )
                .and( lte( COLUMN1, bindMarker() ) );


        getString = session.prepare(  whereString );

        Select lastString = select()
                .all()
                .from( CassandraModel.CF_METRICS_STRING_NAME )
                .where( eq( KEY, bindMarker() ) )
                .orderBy( desc( COLUMN1 ) )
                .limit( 1 );

        getLastString = session.prepare( lastString );
    }

    public ResultSetFuture getStringAsync( Locator locator, Range range ) {

        BoundStatement bound = getString.bind( locator.toString(), range.getStart(), range.getStop() );

        return DatastaxIO.getSession().executeAsync( bound );
    }

    public ResultSetFuture insertAsync( IMetric metric ) {

        boolean stringOrBool = DataType.isStringOrBoolean( metric.getMetricValue() );

        if (stringOrBool ) {

            BoundStatement bound = putString.bind( metric.getLocator().toString(),
                    metric.getCollectionTime(),
                    String.valueOf( metric.getMetricValue() ),
                    (int) STRING_TTL.toSeconds() );

            return DatastaxIO.getSession().executeAsync( bound );
        }
        else {

            BoundStatement bound = putNumeric.bind( metric.getLocator().toString(),
                    metric.getCollectionTime(),
                    serDes.serialize( metric.getMetricValue() ),
                    metric.getTtlInSeconds() );

            return DatastaxIO.getSession().executeAsync( bound );
        }
    }

    public String getLastStringValue( Locator locator ) {

        Timer.Context ctx = Instrumentation.getReadTimerContext( CassandraModel.CF_METRICS_STRING_NAME );

        try {

            BoundStatement bound = getLastString.bind( locator.toString() );

            List<Row> result = DatastaxIO.getSession().execute(  bound ).all();

            return result.isEmpty() ? null : result.get( 0 ).getString( VALUE );
        }
        catch (Exception e ) {

            Instrumentation.markReadError();

            LOG.error( "Unable to read lastest value from metrics_string for %s", locator, e );
            throw new RuntimeException( e );
        }
        finally {

            ctx.stop();
        }
    }

    public MetricData createMetricDataStringBoolean( ResultSetFuture future, boolean isBoolean, String unit ) {
        Points points = new Points<String>();

        for ( Row row : future.getUninterruptibly().all() ) {

            Object value = isBoolean ? Boolean.valueOf( row.getString( VALUE ) ) : row.getString( VALUE );
            points.add( new Points.Point( row.getLong( COLUMN1 ), value ) );
        }

        MetricData.Type type = isBoolean ? MetricData.Type.BOOLEAN : MetricData.Type.BOOLEAN;

        return new MetricData( points, unit, type );
    }

}
