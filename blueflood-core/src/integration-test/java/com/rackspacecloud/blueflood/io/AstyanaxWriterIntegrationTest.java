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

package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.types.Locator;
import org.junit.Test;

public class AstyanaxWriterIntegrationTest extends IntegrationTestBase {

    @Test
    public void testEnsureStringMetricsDoNotEndUpInNumericSpace() throws Exception {
        assertNumberOfRows("metrics_string", 0);
        assertNumberOfRows("metrics_full", 0);
        assertNumberOfRows("metrics_locator", 0);

        writeMetric("string_metric", "This is a string test");
        
        assertNumberOfRows("metrics_string", 1);
        assertNumberOfRows("metrics_full", 0);
        assertNumberOfRows("metrics_locator", 0);
    }

    @Test
    public void testEnsureNumericMetricsDoNotEndUpInStringSpaces() throws Exception {
        assertNumberOfRows("metrics_string", 0);
        assertNumberOfRows("metrics_full", 0);
        assertNumberOfRows("metrics_locator", 0);

        writeMetric("long_metric", 64L);
        
        assertNumberOfRows("metrics_string", 0);
        assertNumberOfRows("metrics_full", 1);
        assertNumberOfRows("metrics_locator", 1);
    }

    @Test
    public void testMetadataGetsWritten() throws Exception {
        assertNumberOfRows("metrics_metadata", 0);

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

        assertNumberOfRows("metrics_metadata", 1);

        // new locator means new row.
        writer.writeMetadataValue(loc2, "a", "strrrrring");
        assertNumberOfRows("metrics_metadata", 2);
    }
}
