/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.cache;

import com.rackspacecloud.blueflood.io.astyanax.ACassandraUtilsIO;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.MetadataIO;
import com.rackspacecloud.blueflood.io.astyanax.AMetadataIO;
import com.rackspacecloud.blueflood.io.datastax.DMetadataIO;
import com.rackspacecloud.blueflood.test.CassandraUtilsIO;
import com.rackspacecloud.blueflood.io.datastax.DCassandraUtilsIO;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.MetricMetadata;
import com.rackspacecloud.blueflood.io.InMemoryMetadataIO;
import com.rackspacecloud.blueflood.utils.TimeValue;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MetadataCacheIntegrationTest extends IntegrationTestBase {

    private final MetadataIO metadataIO;
    private final CassandraUtilsIO cassandraUtilsIO;
    
    public MetadataCacheIntegrationTest(MetadataIO mIO, CassandraUtilsIO tIO) {
        metadataIO = mIO;
        cassandraUtilsIO = tIO;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        // equivalent of database truncate.
        if ( metadataIO instanceof InMemoryMetadataIO) {
            ((InMemoryMetadataIO) metadataIO ).backingTable.clear();
        }
    }
    
    @Test
    public void testPut() throws Exception {
        Assert.assertEquals( 0, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_METADATA_NAME ) );
        
        MetadataCache cache = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        cache.setIO( metadataIO );
        Locator loc1 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "acOne", "ent", "chk", "mz", "met");
        Locator loc2 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "acTwo", "ent", "chk", "mz", "met");
        cache.put(loc1, "metaA", "some string");
        cache.put(loc1, "metaB", "fooz");
        cache.put(loc1, "metaC", "some other string");

        Assert.assertEquals( 1, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_METADATA_NAME ) );

        cache.put(loc2, "metaA", "hello");
        
        Assert.assertEquals( 2, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_METADATA_NAME ) );
    }


    @Test
    public void testGetNull() throws Exception {
        Locator loc1 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "acOne", "ent", "chk", "mz", "met");
        MetadataCache cache1 = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        cache1.setIO( metadataIO );
        Assert.assertNull(cache1.get(loc1, "foo"));
        Assert.assertNull(cache1.get(loc1, "foo"));
    }

    @Test
    @Ignore
    //Ignoring this test case as it does not assert anything and modifies the static instance of MetaDataCache
    //TODO Add some logical implentation for this test case.
    public void testCollisions() throws Exception {
        Locator loc1 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "ac76PeGPSR", "entZ4MYd1W", "chJ0fvB5Ao", "mzord", "truncated"); // put unit of bytes
        Locator loc2 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "acTmPLSgfv", "enLctkAMeN", "chQwBe5YiE", "mzdfw", "cert_end_in"); // put type of I

        MetadataCache cache = MetadataCache.getInstance();
        cache.setIO( metadataIO );

        cache.put(loc1, MetricMetadata.UNIT.name().toLowerCase(), "foo");
    }

    @Test
    public void testGet() throws Exception {
        Locator loc1 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "acOne", "ent", "chk", "mz", "met");
        MetadataCache cache1 = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        MetadataCache cache2 = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        
        cache1.setIO( metadataIO );
        cache2.setIO( metadataIO );
        
        // put in one, read in both.
        Class<String> expectedClass = String.class;
        String expected = "expected";

        String key = "metaA";
        cache1.put(loc1, key, expected);
        Assert.assertEquals(expected, cache1.get(loc1, key, expectedClass));
        Assert.assertEquals(expected, cache2.get(loc1, key, expectedClass));
        
        // update in one verify can only new value there.
        expected = "different expected";
        Assert.assertFalse(expected.equals(cache1.get(loc1, key, expectedClass)));
        cache1.put(loc1, key, expected);
        Assert.assertEquals(expected, cache1.get(loc1, key, expectedClass));
        
        // cache2 has old value that is unexpired. invalidate and read new value.
        Assert.assertFalse(expected.equals(cache2.get(loc1, key, expectedClass)));
        cache2.invalidate(loc1, key);
        Assert.assertEquals(expected, cache2.get(loc1, key, expectedClass));
        
        // re-read on invalidate.
        cache1.invalidate(loc1, key);
        Assert.assertFalse(cache1.containsKey(loc1, key));
        Assert.assertEquals(expected, cache1.get(loc1, key));
    }

    @Test
    public void testPutsAreNotDuplicative() throws Exception {
        Locator loc1 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "acOne", "ent", "chk", "mz", "met");
        MetadataCache cache1 = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        cache1.setIO( metadataIO );
        String key = "metaA";
        String v1 = new String("Hello");
        String v2 = new String("Hello");
        
        Assert.assertTrue(v1 != v2);
        Assert.assertEquals(v1, v2);
        Assert.assertTrue(cache1.put(loc1, key, v1));
        Assert.assertFalse(cache1.put(loc1, key, v2));
    }

    @Test
    public void testExpiration() throws Exception {
        Locator loc1 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "acOne", "ent.chk.mz.met");

        MetadataCache cache1 = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        MetadataCache cache2 = MetadataCache.createLoadingCacheInstance(new TimeValue(3, TimeUnit.SECONDS), 1);
        
        cache1.setIO( metadataIO );
        cache2.setIO( metadataIO );
        
        // update in 1, should read out of both.
        Class<String> expectedClass = String.class;
        String expected = "Hello";
        String key = "metaA";
        cache1.put(loc1, key, expected);
        Assert.assertEquals(expected, cache1.get(loc1, key, expectedClass));
        Assert.assertEquals(expected, cache2.get(loc1, key, expectedClass));
        
        // update cache1, but not cache2.
        expected = "Hello2";
        Assert.assertFalse(expected.equals(cache1.get(loc1, key, expectedClass)));
        cache1.put(loc1, key, expected);
        Assert.assertEquals(expected, cache1.get(loc1, key, expectedClass));
        // verify that 2 has old value.
        Assert.assertFalse(expected.equals(cache2.get(loc1, key, expectedClass)));
        
        // wait for expiration, then verify that new value is picked up.
        Thread.sleep(4000);
        Assert.assertEquals(expected, cache2.get(loc1, key, expectedClass));
    }

    @Test
    public void testTypedGet() throws Exception {
        MetadataCache cache = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        cache.setIO( metadataIO );
        Locator loc1 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "acOne", "ent", "chk", "mz", "met");
        String expectedString = "expected";
        
        cache.put(loc1, "str", expectedString);

        Assert.assertEquals(expectedString, cache.get(loc1, "str", String.class));
    }
    
    @Test
    public void testIOReplacement() throws Exception {
        
        // create the replacement IO.
        final MetadataIO mapIO = new InMemoryMetadataIO();
        final MetadataIO astIO = new AMetadataIO();
        
        final MetadataCache cache = MetadataCache.createLoadingCacheInstance();
        cache.setIO(astIO);
        
        // DO NOT SET USING LOCAL IO INSTANCE!!!!
        
        // put an get a value with the old IO
        Locator loc = Locator.createLocatorFromPathComponents( getRandomTenantId(), "io_replacment", "a", "b", "c");
        Assert.assertNull(cache.get(loc, "foo"));
        cache.put(loc, "foo", "bar");
        Assert.assertNotNull(cache.get(loc, "foo"));
        Assert.assertEquals("bar", cache.get(loc, "foo"));
        
        // replace the IO, ensure there is nothing there, do a put and get, verify they are different than from before.
        cache.setIO(mapIO);
        Assert.assertNull(cache.get(loc, "foo"));
        cache.put(loc, "foo", "baz");
        Assert.assertNotNull(cache.get(loc, "foo"));
        Assert.assertEquals("baz", cache.get(loc, "foo"));
        
        // put the old IO back. this should result in the old value being read from the cache.
        cache.setIO(astIO);
        Assert.assertEquals("bar", cache.get(loc, "foo"));
    }
    
    @Test
    public void testPersistence() throws Exception {
        MetadataCache cache0 = MetadataCache.createLoadingCacheInstance();
        cache0.setIO(new InMemoryMetadataIO());
        
        Locator l0 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "1", "a", "b");
        Locator l1 = Locator.createLocatorFromPathComponents( getRandomTenantId(), "1", "c", "d");
        cache0.put(l0, "foo" , "l0_foo");
        cache0.put(l0, "bar", "l0_bar");
        cache0.put(l1, "zee", "zzzzz");
        
        File f = File.createTempFile("metadatacache_persistence", "txt");
        f.deleteOnExit();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(f, false));
        
        cache0.save(out);
        out.close();
        
        MetadataCache cache1 = MetadataCache.createLoadingCacheInstance();
        cache1.setIO(new InMemoryMetadataIO());
        
        // verify nothing is in the cache.
        Assert.assertNull(cache1.get(l0, "foo"));
        Assert.assertNull(cache1.get(l0, "bar"));
        Assert.assertNull(cache1.get(l1, "zee"));
        
        // now load it.
        DataInputStream in = new DataInputStream(new FileInputStream(f));
        cache1.load(in);
        
        Assert.assertEquals("l0_foo", cache1.get(l0, "foo"));
        Assert.assertEquals("l0_bar", cache1.get(l0, "bar"));
        Assert.assertEquals("zzzzz", cache1.get(l1, "zee"));
    }
    

    @Parameterized.Parameters
    public static Collection<Object[]> getIOs() {
        List<Object[]> ios = new ArrayList<Object[]>();
        ios.add(new Object[] { new AMetadataIO(), new ACassandraUtilsIO() });
        ios.add(new Object[] { new DMetadataIO(), new DCassandraUtilsIO() });

        InMemoryMetadataIO memIO = new InMemoryMetadataIO();
        ios.add(new Object[] { memIO, memIO });

        return ios;
    }
}
