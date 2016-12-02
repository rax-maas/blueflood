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

}
