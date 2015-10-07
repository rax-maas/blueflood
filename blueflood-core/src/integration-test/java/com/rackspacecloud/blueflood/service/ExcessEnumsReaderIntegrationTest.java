/*
 * Copyright 2015 Rackspace
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

package com.rackspacecloud.blueflood.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.types.Locator;


public class ExcessEnumsReaderIntegrationTest extends IntegrationTestBase {
    Locator dummyLocator = Locator.createLocatorFromPathComponents("abc","def");
    final Thread eerThread = new Thread(ExcessEnumReader.getInstance(), "Excess Enum Table Reader");

    @Before
    public void setUp() throws Exception {
        AstyanaxWriter.getInstance().writeExcessEnumMetric(dummyLocator, 0L);
    }


    @Test
    public void testReader() throws Exception {
        // Before the table is read from Cassandra the locator should not be found
        Assert.assertFalse(ExcessEnumReader.getInstance().isInExcessEnumMetrics(dummyLocator));
        // Start the thread to read the table from Cassandra
        eerThread.start();
        Thread.sleep(100);
        // Confirm that the locator has been read from Cassandra
        Assert.assertTrue(ExcessEnumReader.getInstance().isInExcessEnumMetrics(dummyLocator));

    }
}




