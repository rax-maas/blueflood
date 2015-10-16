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

package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.http.HttpClientVendor;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.*;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.*;

import java.util.*;

public class HttpEnumRollupIntegrationTest extends IntegrationTestBase {
    // A timestamp 2 days ago
    private final long baseMillis = Calendar.getInstance().getTimeInMillis() - 172800000;
    private final String tenantId = "ac" + IntegrationTestBase.randString(8);
    private final String enumMetricName = "enumMet_"+IntegrationTestBase.randString(8);

    List<Locator> enumLocators;
    private static int queryPort = 20000;
    private static HttpQueryService httpQueryService;
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;

    private HttpRollupsQueryHandler httpHandler;
    private final Map<Locator, Map<Granularity, Integer>> enumlocatorToPoints = new HashMap<Locator, Map<Granularity,Integer>>();


    @BeforeClass
    public static void setUpHttp() {
        queryPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT);
        httpQueryService = new HttpQueryService();
        httpQueryService.startService();
        vendor = new HttpClientVendor();
        client = vendor.getClient();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        AstyanaxWriter writer = AstyanaxWriter.getInstance();

        enumLocators = new ArrayList<Locator>();
        for(int i=0; i< 1440; i++) {
            final long curMillis = baseMillis + ( i* 60000);
            final List<IMetric> metrics = new ArrayList<IMetric>();
            final IMetric enumMetric = getEnumMetric(enumMetricName, tenantId, curMillis);
            metrics.add(enumMetric);
            enumLocators.add(enumMetric.getLocator());

            MetadataCache.getInstance().put(enumMetric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), null);
            MetadataCache.getInstance().put(enumMetric.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.ENUM.toString());
            writer.insertMetrics(metrics, CassandraModel.CF_METRICS_PREAGGREGATED_FULL);
        }

        httpHandler = new HttpRollupsQueryHandler();

        // generate every level of rollup for the raw data
        Granularity g = Granularity.FULL;
        while (g != Granularity.MIN_1440) {
            g = g.coarser();
            for(Locator locator : enumLocators) {
                generateEnumRollups(locator, baseMillis, baseMillis + 86400000, g);
            }
        }

        final Map<Granularity, Integer> answerForEnumMetric = new HashMap<Granularity, Integer>();
        answerForEnumMetric.put(Granularity.FULL, 1440);
        answerForEnumMetric.put(Granularity.MIN_5, 289);
        answerForEnumMetric.put(Granularity.MIN_20, 73);
        answerForEnumMetric.put(Granularity.MIN_60, 25);
        answerForEnumMetric.put(Granularity.MIN_240, 7);
        answerForEnumMetric.put(Granularity.MIN_1440, 2);

        enumlocatorToPoints.put(enumLocators.get(0), answerForEnumMetric);
    }

    @Test
    public void testEnumRollups() throws Exception {
        testGetRollupByPointsEnums();
        testGetRollupByResolution();
    }

    private void testGetRollupByPointsEnums() throws Exception {
        final Map<Granularity, Integer> points = new HashMap<Granularity, Integer>();
        points.put(Granularity.FULL, 1600);
        points.put(Granularity.MIN_5, 287);
        points.put(Granularity.MIN_20, 71);
        points.put(Granularity.MIN_60, 23);
        points.put(Granularity.MIN_240, 5);
        points.put(Granularity.MIN_1440, 1);

        HttpRollupHandlerIntegrationTest.testHTTPRollupHandlerGetByPoints(enumlocatorToPoints, points, baseMillis, baseMillis + 86400000, enumLocators, httpHandler);
    }

    private void testGetRollupByResolution() throws Exception {
        HttpRollupHandlerIntegrationTest.testGetRollupByResolution(enumLocators, enumlocatorToPoints, httpHandler);
    }

    @AfterClass
    public static void shutdown() {
        if (vendor != null) {
            vendor.shutdown();
        }

        if (httpQueryService != null) {
            httpQueryService.stopService();
        }
    }
}
