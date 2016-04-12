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

    @Override
    public Map<String, String> getAllValues( Locator locator ) throws IOException {

        Timer.Context ctx = Instrumentation.getReadTimerContext( CassandraModel.CF_METRICS_METADATA_NAME );

        Session session = DatastaxIO.getSession();

        try {

            Select.Where select = select()
                    .all()
                    .from( CassandraModel.CF_METRICS_METADATA_NAME )
                    .where( eq( KEY, locator.toString() ));

            List<Row> results = session.execute( select ).all();

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

            List<String> query = new ArrayList<String>();

            for( Locator l : locators) {
                query.add( l.toString() );
            }

            Select.Where select = select()
                    .all()
                    .from( CassandraModel.CF_METRICS_METADATA_NAME )
                    .where( in( KEY, query ) );

            List<Row> results = session.execute( select ).all();

            Table<Locator, String, String> metaTable = HashBasedTable.create();

            for ( Row row : results ) {
                if ( LOG.isTraceEnabled() ) {
                    LOG.trace( "Read metrics_metadata: " +
                            row.getString( KEY ) +
                            row.getString( COLUMN1 ) +
                            serDes.deserialize(  row.getBytes( VALUE ) ) );
                }

                metaTable.put( Locator.createLocatorFromDbKey( row.getString( KEY ) ), row.getString( COLUMN1 ), serDes.deserialize(  row.getBytes( VALUE ) ) );
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
            Insert insert = insertInto( CassandraModel.CF_METRICS_METADATA_NAME )
                    .value( KEY, locator.toString() )
                    .value( COLUMN1, key )
                    .value( VALUE, serDes.serialize( value ) );
            // TODO: This is required by the cassandra-maven-plugin 2.0.0-1, but not by cassandra 2.0.11, which we run.
            // I believe its due to the bug https://issues.apache.org/jira/browse/CASSANDRA-6238
            insert.setConsistencyLevel( ConsistencyLevel.ONE );

            ResultSet result = session.execute( insert );
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

        // TODO:  We need to look into doing async calls on each row.  Batch supposedly is stupid in Cassandra
        // when your keys aren't on the same partition
        try {
            PreparedStatement ps = session.prepare( "INSERT INTO " + CassandraModel.CF_METRICS_METADATA_NAME + " (key, column1, value) VALUES (?, ?, ?)" );
            BatchStatement batch = new BatchStatement();

            for( Table.Cell<Locator, String, String> cell : meta.cellSet() ) {

                batch.add( ps.bind( cell.getRowKey().toString(), cell.getColumnKey(), serDes.serialize( cell.getValue() ) ) );
            }

            ResultSet result = session.execute( batch );
            LOG.debug( "result.size=" + result.all().size() );
        }
        finally {
            ctx.stop();
        }
    }
}
