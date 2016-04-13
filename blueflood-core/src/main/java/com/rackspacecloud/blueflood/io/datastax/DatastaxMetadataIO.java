package com.rackspacecloud.blueflood.io.datastax;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.io.MetadataIO;
import com.rackspacecloud.blueflood.io.serializers.metrics.StringMetadataSerDes;
import com.rackspacecloud.blueflood.types.Locator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * Read/write to metrics_metadata using Datastax driver.
 */
public class DatastaxMetadataIO implements MetadataIO {

    public static final String KEY = "key";
    public static final String COLUMN1 = "column1";
    public static final String VALUE = "value";

    private static final Logger LOG = LoggerFactory.getLogger( DatastaxMetadataIO.class );

    private final StringMetadataSerDes serDes = new StringMetadataSerDes();

    private PreparedStatement getValue;
    private PreparedStatement putValue;

    public DatastaxMetadataIO() {

        createPreparedStatements();
    }

    private void createPreparedStatements() {

        Select.Where select = select()
                .all()
                .from( CassandraModel.CF_METRICS_METADATA_NAME )
                .where( eq( KEY, bindMarker() ));

        getValue = DatastaxIO.getSession().prepare( select );

        Insert insert = insertInto( CassandraModel.CF_METRICS_METADATA_NAME )
                .value( KEY, bindMarker() )
                .value( COLUMN1, bindMarker() )
                .value( VALUE, bindMarker() );

        putValue = DatastaxIO.getSession().prepare( insert );

        // TODO: This is required by the cassandra-maven-plugin 2.0.0-1, but not by cassandra 2.0.11, which we run.
        // I believe its due to the bug https://issues.apache.org/jira/browse/CASSANDRA-6238
        putValue.setConsistencyLevel( ConsistencyLevel.ONE );
    }

    @Override
    public Map<String, String> getAllValues( Locator locator ) throws IOException {

        Timer.Context ctx = Instrumentation.getReadTimerContext( CassandraModel.CF_METRICS_METADATA_NAME );

        Session session = DatastaxIO.getSession();

        try {

            BoundStatement bound = getValue.bind( locator.toString() );

            List<Row> results = session.execute( bound ).all();

            Map<String, String> values = new HashMap<String, String>();

            for ( Row row : results ) {
                if ( LOG.isTraceEnabled() ) {
                    LOG.trace( "Read metrics_metadata: " +
                            row.getString( KEY ) +
                            row.getString( COLUMN1 ) +
                            serDes.deserialize(  row.getBytes( VALUE ) ) );
                }

                values.put( row.getString( COLUMN1 ), serDes.deserialize(  row.getBytes( VALUE ) ) );
            }

            return values;
        }
        finally {
            ctx.stop();
        }
    }

    @Override
    public Table<Locator, String, String> getAllValues( Set<Locator> locators ) throws IOException {

        Timer.Context ctx = Instrumentation.getReadTimerContext( CassandraModel.CF_METRICS_METADATA_NAME );

        Session session = DatastaxIO.getSession();

        try {

            List<ResultSetFuture> futureList = new ArrayList<ResultSetFuture>();

            for( Locator l : locators) {

                BoundStatement bound = getValue.bind( l.toString() );

                futureList.add( session.executeAsync( bound ) );
            }

            Table<Locator, String, String> metaTable = HashBasedTable.create();

            for( ResultSetFuture future : futureList ) {

                ResultSet result = future.getUninterruptibly();

                for ( Row row : result ) {
                    if ( LOG.isTraceEnabled() ) {
                        LOG.trace( "Read metrics_metadata: " +
                                row.getString( KEY ) +
                                row.getString( COLUMN1 ) +
                                serDes.deserialize( row.getBytes( VALUE ) ) );
                    }

                    metaTable.put( Locator.createLocatorFromDbKey( row.getString( KEY ) ), row.getString( COLUMN1 ), serDes.deserialize( row.getBytes( VALUE ) ) );
                }
            }

            return metaTable;
        }
        finally {
            ctx.stop();
        }
    }

    @Override
    public void put( Locator locator, String key, String value ) throws IOException {

        Timer.Context ctx = Instrumentation.getWriteTimerContext( CassandraModel.CF_METRICS_METADATA_NAME );

        Session session = DatastaxIO.getSession();

        try {

            BoundStatement bound = putValue.bind( locator.toString(), key, serDes.serialize( value ) );

            ResultSet result = session.execute( bound );
            LOG.trace( "result.size=" + result.all().size() );
        }
        finally {
            ctx.stop();
        }
    }

    @Override
    public void putAll( Table<Locator, String, String> meta ) throws IOException {

        Timer.Context ctx = Instrumentation.getWriteTimerContext( CassandraModel.CF_METRICS_METADATA_NAME );

        Session session = DatastaxIO.getSession();

        List<ResultSetFuture> futureList = new ArrayList<ResultSetFuture>();

        try {
            for( Table.Cell<Locator, String, String> cell : meta.cellSet() ) {

                BoundStatement bound = putValue.bind( cell.getRowKey().toString(), cell.getColumnKey(), serDes.serialize( cell.getValue() ) );

                futureList.add( session.executeAsync( bound ) );
            }

            for( ResultSetFuture future : futureList ) {

                ResultSet result = future.getUninterruptibly();
                LOG.trace( "result.size=" + result.all().size() );
            }
        }
        finally {
            ctx.stop();
        }
    }
}
