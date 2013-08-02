package com.cloudkick.blueflood.cache;

import com.cloudkick.blueflood.types.Locator;
import com.cloudkick.blueflood.io.IntegrationTestBase;
import com.cloudkick.blueflood.types.MetricMetadata;
import com.cloudkick.blueflood.utils.TimeValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class MetadataCacheIntegrationTest extends IntegrationTestBase {

    @Test
    public void testPut() throws Exception {
        assertNumberOfRows("metrics_metadata", 0);
        
        MetadataCache cache = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        Locator loc1 = Locator.createLocatorFromPathComponents("acOne", "ent", "chk", "mz", "met");
        Locator loc2 = Locator.createLocatorFromPathComponents("acTwo", "ent", "chk", "mz", "met");
        cache.put(loc1, "metaA", "some string");
        cache.put(loc1, "metaB", "fooz");
        cache.put(loc1, "metaC", "some other string");

        assertNumberOfRows("metrics_metadata", 1);
        
        cache.put(loc2, "metaA", "hello");
        assertNumberOfRows("metrics_metadata", 2);
    }

    @Test
    public void testGetNull() throws Exception {
        Locator loc1 = Locator.createLocatorFromPathComponents("acOne", "ent", "chk", "mz", "met");
        MetadataCache cache1 = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        Assert.assertNull(cache1.get(loc1, "foo"));
        Assert.assertNull(cache1.get(loc1, "foo"));
    }

    @Test
    public void testCollisions() throws Exception {
        Locator loc1 = Locator.createLocatorFromPathComponents("ac76PeGPSR", "entZ4MYd1W", "chJ0fvB5Ao", "mzord", "truncated"); // put unit of bytes
        Locator loc2 = Locator.createLocatorFromPathComponents("acTmPLSgfv", "enLctkAMeN", "chQwBe5YiE", "mzdfw", "cert_end_in"); // put type of I

        MetadataCache cache = MetadataCache.getInstance();

        cache.put(loc1, MetricMetadata.UNIT.name().toLowerCase(), "foo");
        String str = cache.get(loc2, MetricMetadata.TYPE.name().toLowerCase(), String.class);
        Assert.assertEquals(str, null); // This catches a bug where the hashCode of these two cache keys is identical. (loc2 type == loc1 unit)
    }

    @Test
    public void testGet() throws Exception {
        Locator loc1 = Locator.createLocatorFromPathComponents("acOne", "ent", "chk", "mz", "met");
        MetadataCache cache1 = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        MetadataCache cache2 = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        
        // put in one, read in both.
        Class<String> expectedClass = String.class;
        Object expected = "expected";

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
        Locator loc1 = Locator.createLocatorFromPathComponents("acOne", "ent", "chk", "mz", "met");
        MetadataCache cache1 = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
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
        Locator loc1 = Locator.createLocatorFromPathComponents("acOne", "ent.chk.mz.met");

        MetadataCache cache1 = MetadataCache.createLoadingCacheInstance(new TimeValue(5, TimeUnit.MINUTES), 1);
        MetadataCache cache2 = MetadataCache.createLoadingCacheInstance(new TimeValue(3, TimeUnit.SECONDS), 1);
        
        // update in 1, should read out of both.
        Class<String> expectedClass = String.class;
        Object expected = "Hello";
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
        Locator loc1 = Locator.createLocatorFromPathComponents("acOne", "ent", "chk", "mz", "met");
        // TODO: uncomment, pending https://issues.rax.io/browse/CMD-139
//        Integer expectedInt = 23;
//        Double expectedDouble = 43d;
//        Long expectedLong = 65L;
//        Float expectedFloat = 87f;
//        byte[] expectedBinary = new byte[]{1,2,3,4,5};
        String expectedString = "expected";
        
//        cache.put(loc1, "int", expectedInt);
//        cache.put(loc1, "dbl", expectedDouble);
//        cache.put(loc1, "long", expectedLong);
//        cache.put(loc1, "flt", expectedFloat);
//        cache.put(loc1, "bin", expectedBinary);
        cache.put(loc1, "str", expectedString);

//        Assert.assertArrayEquals(expectedBinary, cache.get(loc1, "bin", byte[].class));
//        assertEquals(expectedInt, cache.get(loc1, "int", Integer.class));
//        assertEquals(expectedDouble, cache.get(loc1, "dbl", Double.class));
//        assertEquals(expectedLong, cache.get(loc1, "long", Long.class));
//        assertEquals(expectedFloat, cache.get(loc1, "flt", Float.class));
        Assert.assertEquals(expectedString, cache.get(loc1, "str", String.class));
    }
}
