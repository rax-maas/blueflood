package com.rackspacecloud.blueflood.io;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.io.astyanax.AMetadataIO;
import com.rackspacecloud.blueflood.io.datastax.DMetadataIO;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.MetricMetadata;
import com.rackspacecloud.blueflood.types.RollupType;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Verify the MetadataIO implementations and ensure Astyanax & Datastax are compatible.
 */
public class MetadataIOIntegrationTest extends IntegrationTestBase {

    private static final String CACHE_KEY = MetricMetadata.ROLLUP_TYPE.name().toLowerCase();

    private AMetadataIO astyanaxMetadataIO = new AMetadataIO();
    private DMetadataIO dMetadataIO = new DMetadataIO();


    @Test
    public void writeSingleAstyanaxReadSingleDatastax() throws IOException {

        Locator name = Locator.createLocatorFromPathComponents( getRandomTenantId(), "single.put.astyanax.single.read.datastax" );

        astyanaxMetadataIO.put( name, CACHE_KEY, RollupType.COUNTER.toString() );

        Map<String, String> result = dMetadataIO.getAllValues( name );

        assertEquals( 1, result.size() );
        Map.Entry<String, String> entry = result.entrySet().iterator().next();

        assertEquals( CACHE_KEY, entry.getKey() );
        assertEquals( RollupType.COUNTER.toString(), entry.getValue() );
    }

    @Test
    public void writeSingleDatastaxReadSingleAstyanax() throws IOException {

        Locator name = Locator.createLocatorFromPathComponents( getRandomTenantId(), "single.put.datastax.single.read.astyanax" );

        dMetadataIO.put( name, CACHE_KEY, RollupType.TIMER.toString() );

        Map<String, String> result = astyanaxMetadataIO.getAllValues( name );

        assertEquals( 1, result.size() );
        Map.Entry<String, String> entry = result.entrySet().iterator().next();

        assertEquals( CACHE_KEY, entry.getKey() );
        assertEquals( RollupType.TIMER.toString(), entry.getValue() );
    }


    @Test
    public void writeAllAstyanaxReadAllDatastax() throws IOException {

        Locator l0 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "all.put.astyanax.all.read.datastax.l0" );
        Locator l1 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "all.put.astyanax.all.read.datastax.l1" );

        Table<Locator, String, String> meta = HashBasedTable.create();
        meta.put( l0, CACHE_KEY, RollupType.GAUGE.toString() );
        meta.put( l1, CACHE_KEY, RollupType.ENUM.toString() );

        astyanaxMetadataIO.putAll( meta );

        Set<Locator> query = new HashSet<Locator>( Arrays.asList( l0, l1 ) );
        Table<Locator, String, String> result = dMetadataIO.getAllValues( query );

        assertEquals( 2, result.size() );

        Map<String, String> row = result.row( l0 );
        assertEquals( 1, row.size() );

        Map.Entry<String, String> entry = row.entrySet().iterator().next();
        assertEquals( CACHE_KEY, entry.getKey() );
        assertEquals( RollupType.GAUGE.toString(), entry.getValue() );

        Map<String, String> row2 = result.row( l1 );
        assertEquals( 1, row2.size() );

        Map.Entry<String, String> entry2 = row2.entrySet().iterator().next();
        assertEquals( CACHE_KEY, entry2.getKey() );
        assertEquals( RollupType.ENUM.toString(), entry2.getValue() );

    }

    @Test
    public void writeAllDatastaxReadAllAstyanax() throws IOException {

        Locator l0 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "all.put.datastax.all.read.astyanax.l0" );
        Locator l1 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "all.put.datastax.all.read.astyanax.l1" );

        Table<Locator, String, String> meta = HashBasedTable.create();
        meta.put( l0, CACHE_KEY, RollupType.GAUGE.toString() );
        meta.put( l1, CACHE_KEY, RollupType.ENUM.toString() );

        dMetadataIO.putAll( meta );

        Set<Locator> query = new HashSet<Locator>( Arrays.asList( l0, l1 ) );
        Table<Locator, String, String> result = astyanaxMetadataIO.getAllValues( query );

        assertEquals( 2, result.size() );

        Map<String, String> row = result.row( l0 );
        assertEquals( 1, row.size() );

        Map.Entry<String, String> entry = row.entrySet().iterator().next();
        assertEquals( CACHE_KEY, entry.getKey() );
        assertEquals( RollupType.GAUGE.toString(), entry.getValue() );

        Map<String, String> row2 = result.row( l1 );
        assertEquals( 1, row2.size() );

        Map.Entry<String, String> entry2 = row2.entrySet().iterator().next();
        assertEquals( CACHE_KEY, entry2.getKey() );
        assertEquals( RollupType.ENUM.toString(), entry2.getValue() );

    }
}
