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

import com.rackspacecloud.blueflood.io.IOContainer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.types.Locator;


public class ExcessEnumsReaderIntegrationTest extends IntegrationTestBase {
    Locator dummyLocator = Locator.createLocatorFromPathComponents("abc","def");
    final Thread eerThread = new Thread(ExcessEnumReader.getInstance(), "Excess Enum Table Reader");

    @Test
    public void testReader() throws Exception {

        // precondition
        Assert.assertFalse("Before the table is read from Cassandra the locator should not be found", 
                            ExcessEnumReader.getInstance().isInExcessEnumMetrics(dummyLocator));

        // when:
        // we insert the locator
        IOContainer.fromConfig().getExcessEnumIO().insertExcessEnumMetric(dummyLocator);
        Configuration.getInstance().setProperty(CoreConfig.EXCESS_ENUM_READER_SLEEP, "1000");

        // and start the thread to read the table from Cassandra
        eerThread.start();
        Thread.sleep(2000);

        // then:
        Assert.assertTrue("After the table is read from Cassandra the locator should be found", 
            ExcessEnumReader.getInstance().isInExcessEnumMetrics(dummyLocator));

    }
}




