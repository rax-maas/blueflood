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

package com.rackspacecloud.blueflood.io;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AstyanaxMetadataIOIntegrationTest extends IntegrationTestBase {

    @Test
    public void testBatchedMetaWritesAndReads() throws Exception {
        final AstyanaxMetadataIO metadataIO = new AstyanaxMetadataIO();
        Table<Locator, String, String> metaTable = HashBasedTable.create();
        final Set<Locator> locators = new HashSet<Locator>();
        MetadataCache cache = MetadataCache.getInstance();

        for (int i = 0; i < 10; i++) {
            Locator loc = Locator.createLocatorFromDbKey(
                    "12345.rackspace.monitoring.enities.enFoo.check_type.agent.cpu.check.chBar.metric.met" + i);
            locators.add(loc);
            metaTable.put(loc, "key", "value");
        }

        metadataIO.putAll(metaTable); // Writes batch to disk

        Thread.sleep(2000); // wait 2s for batch timer to kick in.

        // Read it back.
        Table<Locator, String, String> metaRead = AstyanaxReader.getInstance().getMetadataValues(locators);

        // Assert that we wrote meta for 10 different locators.
        Assert.assertTrue(metaRead.size() == 10);

        for (Locator locator : metaRead.rowKeySet()) {
            Map<String, String> metaMapForLocator = metaRead.row(locator);

            Assert.assertTrue(metaMapForLocator.size() == 1);
            Assert.assertTrue(metaMapForLocator.get("key").equals("value"));
        }
    }
}
