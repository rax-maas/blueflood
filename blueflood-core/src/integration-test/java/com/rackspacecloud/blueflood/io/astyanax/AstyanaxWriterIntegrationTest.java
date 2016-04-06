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

import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.io.TestIO;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.Test;

import static org.junit.Assert.*;


public class AstyanaxWriterIntegrationTest extends IntegrationTestBase {

    private TestIO testIO = new AstyanaxTestIO();

    @Test
    public void testEnsureStringMetricsDoNotEndUpInNumericSpace() throws Exception {

        assertEquals( 0, testIO.getKeyCount( CassandraModel.CF_METRICS_STRING_NAME ) );
        assertEquals( 0, testIO.getKeyCount( CassandraModel.CF_METRICS_FULL_NAME ) );
        assertEquals( 0, testIO.getKeyCount( CassandraModel.CF_METRICS_LOCATOR_NAME ) );

        writeMetric("string_metric", "This is a string test");

        assertEquals( 1, testIO.getKeyCount( CassandraModel.CF_METRICS_STRING_NAME ) );
        assertEquals( 0, testIO.getKeyCount( CassandraModel.CF_METRICS_FULL_NAME ) );
        assertEquals( 0, testIO.getKeyCount( CassandraModel.CF_METRICS_LOCATOR_NAME ) );
    }

    @Test
    public void testEnsureNumericMetricsDoNotEndUpInStringSpaces() throws Exception {
        assertEquals( 0, testIO.getKeyCount( CassandraModel.CF_METRICS_STRING_NAME ) );
        assertEquals( 0, testIO.getKeyCount( CassandraModel.CF_METRICS_FULL_NAME ) );
        assertEquals( 0, testIO.getKeyCount( CassandraModel.CF_METRICS_LOCATOR_NAME ) );

        writeMetric("long_metric", 64L);

        assertEquals( 0, testIO.getKeyCount( CassandraModel.CF_METRICS_STRING_NAME ) );
        assertEquals( 1, testIO.getKeyCount( CassandraModel.CF_METRICS_FULL_NAME ) );
        assertEquals( 1, testIO.getKeyCount( CassandraModel.CF_METRICS_LOCATOR_NAME ) );

    }

    @Test
    public void testMetadataGetsWritten() throws Exception {
        assertEquals( 0, testIO.getKeyCount( CassandraModel.CF_METRICS_METADATA_NAME ) );


        Locator loc1 = Locator.createLocatorFromPathComponents("acONE", "entityId", "checkId", "mz", "metric");
        Locator loc2 = Locator.createLocatorFromPathComponents("acTWO", "entityId", "checkId", "mz", "metric");
        AstyanaxWriter writer = AstyanaxWriter.getInstance();

        // multiple cols on a single locator should produce a single row.
//        writer.writeMetadataValue(loc1, "a", new byte[]{1,2,3,4,5});
//        writer.writeMetadataValue(loc1, "b", new byte[]{6,7,8,9,0});
//        writer.writeMetadataValue(loc1, "c", new byte[]{11,22,33,44,55,66});
//        writer.writeMetadataValue(loc1, "d", new byte[]{-1,-2,-3,-4});
//        writer.writeMetadataValue(loc1, "e", new byte[]{1,2,3,4,5});
//        writer.writeMetadataValue(loc1, "f", new byte[]{1,2,3,4,5,6,7,8,9,0});
        writer.writeMetadataValue(loc1, "a", "Some1String");
        writer.writeMetadataValue(loc1, "b", "Some2String");
        writer.writeMetadataValue(loc1, "c", "Some3String");
        writer.writeMetadataValue(loc1, "d", "Some4String");
        writer.writeMetadataValue(loc1, "e", "Some5String");
        writer.writeMetadataValue(loc1, "f", "Some6String");

        assertEquals( 1, testIO.getKeyCount( CassandraModel.CF_METRICS_METADATA_NAME ) );

        // new locator means new row.
        writer.writeMetadataValue(loc2, "a", "strrrrring");
        assertEquals( 2, testIO.getKeyCount( CassandraModel.CF_METRICS_METADATA_NAME ) );
    }

    @Test
    public void testExcessEnumMetricGetsWritten() throws Exception {
        assertEquals( 0, testIO.getKeyCount( CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME ) );

        Locator loc1 = Locator.createLocatorFromPathComponents("acONE", "entityId", "checkId", "mz", "metric");
        Locator loc2 = Locator.createLocatorFromPathComponents("acTWO", "entityId", "checkId", "mz", "metric");
        AstyanaxWriter writer = AstyanaxWriter.getInstance();

        writer.writeExcessEnumMetric(loc1);
        assertEquals( 1, testIO.getKeyCount( CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME ) );

        // new locator means new row.
        writer.writeExcessEnumMetric(loc2);
        assertEquals( 2, testIO.getKeyCount( CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME ) );
    }

    @Test
    public void testExcessEnumMetricDoesNotDuplicates() throws Exception {
        assertEquals( 0, testIO.getKeyCount( CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME ) );

        Locator loc1 = Locator.createLocatorFromPathComponents("ac", "entityId", "checkId", "mz", "metric");
        Locator loc2 = Locator.createLocatorFromPathComponents("ac", "entityId", "checkId", "mz", "metric");
        Locator loc3 = Locator.createLocatorFromPathComponents("ac", "entityId", "checkId", "mz", "metric");
        Locator loc4 = Locator.createLocatorFromPathComponents("acNEW", "entityId", "checkId", "mz", "metric");
        AstyanaxWriter writer = AstyanaxWriter.getInstance();

        // same locator means same row.
        writer.writeExcessEnumMetric(loc1);
        writer.writeExcessEnumMetric(loc2);
        writer.writeExcessEnumMetric(loc3);
        assertEquals( 1, testIO.getKeyCount( CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME ) );

        // new locator means new row.
        writer.writeExcessEnumMetric(loc4);
        assertEquals( 2, testIO.getKeyCount( CassandraModel.CF_METRICS_EXCESS_ENUMS_NAME ) );
    }

}
