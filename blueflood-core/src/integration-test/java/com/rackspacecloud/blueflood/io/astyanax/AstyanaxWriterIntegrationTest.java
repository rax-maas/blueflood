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

package com.rackspacecloud.blueflood.io.astyanax;

import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.Test;

import static org.junit.Assert.*;


public class AstyanaxWriterIntegrationTest extends IntegrationTestBase {

    private CassandraUtilsIO cassandraUtilsIO = new ACassandraUtilsIO();
    ExcessEnumIO excessEnumIO = IOContainer.fromConfig().getExcessEnumIO();

    @Test
    public void testEnsureStringMetricsDoNotEndUpInNumericSpace() throws Exception {

        assertEquals( "Ensure " + CassandraModel.CF_METRICS_STRING_NAME + "is 0 before",
                0, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_STRING_NAME ) );
        assertEquals( "Ensure " + CassandraModel.CF_METRICS_FULL_NAME + "is 0 before",
                0, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_FULL_NAME ) );
        assertEquals( "Ensure " + CassandraModel.CF_METRICS_LOCATOR_NAME + "is 0 before",
                0, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_LOCATOR_NAME ) );

        writeMetric("string_metric", "This is a string test");

        assertEquals( "Ensure " + CassandraModel.CF_METRICS_STRING_NAME + "is 1 after",
                1, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_STRING_NAME ) );
        assertEquals( "Ensure " + CassandraModel.CF_METRICS_FULL + "is 0 after",
                0, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_FULL_NAME ) );
        assertEquals( "Ensure " + CassandraModel.CF_METRICS_LOCATOR_NAME + "is 0 after",
                0, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_LOCATOR_NAME ) );
    }

    @Test
    public void testEnsureNumericMetricsDoNotEndUpInStringSpaces() throws Exception {
        assertEquals( "Ensure " + CassandraModel.CF_METRICS_STRING_NAME + "is 0 before",
                0, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_STRING_NAME ) );
        assertEquals( "Ensure " + CassandraModel.CF_METRICS_FULL_NAME + "is 0 before",
                0, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_FULL_NAME ) );
        assertEquals( "Ensure " + CassandraModel.CF_METRICS_LOCATOR_NAME + "is 0 before",
                0, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_LOCATOR_NAME ) );

        writeMetric("long_metric", 64L);

        assertEquals( "Ensure " + CassandraModel.CF_METRICS_STRING_NAME + "is 0 after",
                0, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_STRING_NAME ) );
        assertEquals( "Ensure " + CassandraModel.CF_METRICS_FULL_NAME + "is 1 after",
                1, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_FULL_NAME ) );
        assertEquals( "Ensure " + CassandraModel.CF_METRICS_LOCATOR_NAME + "is 1 after",
                1, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_LOCATOR_NAME ) );

    }

    @Test
    public void testExcessEnumMetricGetsWritten() throws Exception {
        assertEquals( "Ensure " + CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME + " 0 before",
                0, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME ) );

        Locator loc1 = Locator.createLocatorFromPathComponents("acONE", "entityId", "checkId", "mz", "metric");
        Locator loc2 = Locator.createLocatorFromPathComponents("acTWO", "entityId", "checkId", "mz", "metric");
        AstyanaxWriter writer = AstyanaxWriter.getInstance();

        excessEnumIO.insertExcessEnumMetric(loc1);
        assertEquals(  "Ensure " + CassandraModel.CF_METRICS_ENUM_NAME + " 1 after",
                1, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME ) );

        // new locator means new row.
        excessEnumIO.insertExcessEnumMetric(loc2);
        assertEquals(  "Ensure " + CassandraModel.CF_METRICS_ENUM_NAME + " 2 after",
                2, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME ) );
    }

    @Test
    public void testExcessEnumMetricDoesNotDuplicates() throws Exception {
        assertEquals( "Ensure " + CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME + " 0 before",
                0, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME ) );

        Locator loc1 = Locator.createLocatorFromPathComponents("ac", "entityId", "checkId", "mz", "metric");
        Locator loc2 = Locator.createLocatorFromPathComponents("ac", "entityId", "checkId", "mz", "metric");
        Locator loc3 = Locator.createLocatorFromPathComponents("ac", "entityId", "checkId", "mz", "metric");
        Locator loc4 = Locator.createLocatorFromPathComponents("acNEW", "entityId", "checkId", "mz", "metric");
        AstyanaxWriter writer = AstyanaxWriter.getInstance();

        // same locator means same row.
        excessEnumIO.insertExcessEnumMetric(loc1);
        excessEnumIO.insertExcessEnumMetric(loc2);
        excessEnumIO.insertExcessEnumMetric(loc3);
        assertEquals( "Ensure " + CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME + " 1 after",
                1, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME ) );

        // new locator means new row.
        excessEnumIO.insertExcessEnumMetric(loc4);
        assertEquals( "Ensure " + CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME + " 2 after",
                2, cassandraUtilsIO.getKeyCount( CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME ) );
    }

}
