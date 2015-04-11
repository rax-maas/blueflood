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

package com.rackspacecloud.blueflood.tools;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.types.*;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import com.rackspacecloud.blueflood.tools.ops.FixTTL;
import java.io.IOException;
import java.util.*;
import com.netflix.astyanax.model.*;


public class FixTTLIntegrationTest extends IntegrationTestBase {
    private final AstyanaxReader reader = AstyanaxReader.getInstance();
    private final AstyanaxWriter writer = AstyanaxWriter.getInstance();
    private final String tenantId = "tenantId";
    private List<String> paths = null;
    private final String path1 = "fixTTL.metric.one";
    private final String path2 = "fixTTL.metric.two";

    private final Locator locator1 = Locator.createLocatorFromPathComponents(tenantId, path1);
    private final Locator locator2 = Locator.createLocatorFromPathComponents(tenantId, path2);
    private List<Locator> locators = null;
    private final long curMillis = 1333635148000L; // some point during 5 April 2012.
    private List<Metric> metrics = null;
    private final Range range = new Range(curMillis - 10000, curMillis + 10000);
    private final Integer oldTTL = 60 * 60 * 24;
    private final Integer newTTL = 1234567;
    private final ColumnFamily CF = CassandraModel.CF_METRICS_FULL;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        paths = new ArrayList<String>();
        paths.add(path1);
        paths.add(path2);

        metrics = new ArrayList<Metric>();
        metrics.add(makeMetric(locator1, curMillis,1));
        metrics.add(makeMetric(locator2, curMillis,2));
        writer.insertFull(metrics);

        locators = new ArrayList<Locator>();
        locators.add(locator1);
        locators.add(locator2);
    }

    @Test
    public void testFixTTL() throws Exception {
        checkTTL(true, oldTTL);
        checkTTL(false, newTTL);
    }
    
    public void checkTTL(Boolean dryRun, Integer ttl) throws Exception {
        // Confirm TTL's have match expected values
        FixTTL.fixTTLs(CF, tenantId, paths, range, newTTL, dryRun);
        Map<Locator, ColumnList<Long>> data =
                reader.getColumnsFromDB(locators, CF, range);
        Column<Long> col1 = data.get(locator1).getColumnByName(curMillis);
        Column<Long> col2 = data.get(locator2).getColumnByName(curMillis);

        Assert.assertEquals((int)col1.getTtl(), (int)ttl);
        Assert.assertEquals((int)col2.getTtl(), (int)ttl);
    }
}
