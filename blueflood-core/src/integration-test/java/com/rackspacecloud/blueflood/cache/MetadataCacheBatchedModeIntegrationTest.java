/*
 * Copyright 2014 Rackspace
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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.io.AstyanaxMetadataIO;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class MetadataCacheBatchedModeIntegrationTest extends IntegrationTestBase {

    @Test
    public void testBatchModeWrites() throws Exception {
        System.setProperty(CoreConfig.META_CACHE_BATCHED_WRITES.name(), "true");
        Configuration.getInstance().init();
        // Verify batch write mode is actually on
        Assert.assertTrue(Configuration.getInstance().getBooleanProperty(CoreConfig.META_CACHE_BATCHED_WRITES));

        MetadataCache cache = MetadataCache.createLoadingCacheInstance();

        // Write some data to metadata cache.
        Locator l0 = Locator.createLocatorFromPathComponents("1", "a", "b");
        Locator l1 = Locator.createLocatorFromPathComponents("1", "c", "d");
        cache.put(l0, "foo" , "l0_foo");
        cache.put(l0, "bar", "l0_bar");
        cache.put(l1, "zee", "zzzzz");

        // Wait until the batch timer kicks off and flushes things to disk.
        Thread.sleep(2000);

        // By pass cache and read meta from disk to make sure things got written.
        AstyanaxMetadataIO metadataIO = new AstyanaxMetadataIO();
        Map<String, String> metaForL0 = metadataIO.getAllValues(l0);
        Assert.assertTrue(metaForL0.get("foo").equals("l0_foo"));
        Assert.assertTrue(metaForL0.get("bar").equals("l0_bar"));
    }


    @Test
    public void testBatchModeReads() throws Exception {
        System.setProperty(CoreConfig.META_CACHE_BATCHED_READS.name(), "true");
        Configuration.getInstance().init();
        // Verify batch write mode is actually on
        Assert.assertTrue(Configuration.getInstance().getBooleanProperty(CoreConfig.META_CACHE_BATCHED_READS));

        MetadataCache cache = MetadataCache.createLoadingCacheInstance();

        // Write some data to metadata cache.
        Locator l0 = Locator.createLocatorFromPathComponents("1", "a", "b");
        Locator l1 = Locator.createLocatorFromPathComponents("1", "c", "d");

        AstyanaxMetadataIO metadataIO = new AstyanaxMetadataIO();
        Table<Locator, String, String> metaToWrite = HashBasedTable.create();

        // By the pass the cache and write to disk directly.
        metaToWrite.put(l0, "foo", "l0_foo");
        metaToWrite.put(l0, "bar", "l0_bar");
        metaToWrite.put(l1, "zee", "zzzzz");
        metadataIO.putAll(metaToWrite);

        // Do a cache get on one of those locators. We should get back null immediately.
        Assert.assertNull(cache.get(l0, "foo"));

        // Wait for the cache to be populated async for that locator + meta.
        Thread.sleep(2000);

        Assert.assertTrue(cache.get(l0, "foo").equals("l0_foo"));
        // We should have also read other meta for that locator.
        Assert.assertTrue(cache.get(l0, "bar").equals("l0_bar"));
    }


    // TODO: Ideally, Configuration has setXXX methods so we don't have to do this.
    @After
    public void tearDown() throws Exception {
        System.setProperty(CoreConfig.META_CACHE_BATCHED_WRITES.name(), "false");
        System.setProperty(CoreConfig.META_CACHE_BATCHED_READS.name(), "false");
        Configuration.getInstance().init();
    }
}
