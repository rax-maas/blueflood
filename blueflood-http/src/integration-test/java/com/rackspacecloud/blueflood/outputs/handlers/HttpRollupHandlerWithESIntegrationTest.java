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
import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Util;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 *
 * The current scope gives us one cluster for all test methods in the test.
 * All indices and templates are deleted between each test.
 *
 * The following flags have to be set while running this test
 * -Dtests.jarhell.check=false (to handle some bug in intellij https://github.com/elastic/elasticsearch/issues/14348)
 * -Dtests.security.manager=false (https://github.com/elastic/elasticsearch/issues/16459)
 *
 */
public class HttpRollupHandlerWithESIntegrationTest extends HttpIntegrationTestBase {
    //A time stamp 2 days ago
    private final long baseMillis = Calendar.getInstance().getTimeInMillis() - 172800000;
    private final String tenantId = "ac" + IntegrationTestBase.randString(8);
    private final String metricName = "met_" + IntegrationTestBase.randString(8);
    private final Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricName);
    private Map<Granularity, Integer> granToPoints = new HashMap<Granularity,Integer>();
    private HttpRollupsQueryHandler httpHandler;
    private static ElasticIO elasticIO;
    IMetric metric;

    @Before
    public void setup() throws Exception {

        super.esSetup();
        ((EventElasticSearchIO) eventsSearchIO).setClient(getClient());

        MetricsRW metricsRW = IOContainer.fromConfig().getBasicMetricsRW();
        IncomingMetricMetadataAnalyzer analyzer = new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());

        final List<IMetric> metrics = new ArrayList<IMetric>();
        for (int i = 0; i < 1440; i++) {
            final long curMillis = baseMillis + i * 60000;
            final Metric metric = getRandomIntMetric(locator, curMillis);
            metrics.add(metric);
        }

        elasticIO = new ElasticIO(getClient());

        elasticIO.insertDiscovery(new ArrayList<IMetric>(metrics));
        refreshChanges();

        analyzer.scanMetrics(new ArrayList<IMetric>(metrics));
        metricsRW.insertMetrics(metrics);

        httpHandler = new HttpRollupsQueryHandler();

        // generate every level of rollup for the raw data
        Granularity g = Granularity.FULL;
        while (g != Granularity.MIN_1440) {
            g = g.coarser();
            generateRollups(locator, baseMillis, baseMillis + 86400000, g);
        }

        AbstractMetricsRW preaggregatedRW = IOContainer.fromConfig().getPreAggregatedMetricsRW();
        metric = writeGaugeMetric(preaggregatedRW, "gauge_metric2", "333333");
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.GAUGE.toString());

        granToPoints.put(Granularity.FULL, 1440);
        granToPoints.put(Granularity.MIN_5, 289);
        granToPoints.put(Granularity.MIN_20, 73);
        granToPoints.put(Granularity.MIN_60, 25);
        granToPoints.put(Granularity.MIN_240, 7);
        granToPoints.put(Granularity.MIN_1440, 2);
    }

    @Test
    public void testOldMetricDataFetching() throws Exception {
        final Map<Granularity, Integer> points = new HashMap<Granularity, Integer>();
        //long currentTimeStamp = Calendar.getInstance().getTimeInMillis();
        long millisInADay = 86400 * 1000;

        points.put(Granularity.FULL, 1600);
        points.put(Granularity.MIN_5, 400);
        points.put(Granularity.MIN_20, 71);
        points.put(Granularity.MIN_60, 23);
        points.put(Granularity.MIN_240, 5);
        points.put(Granularity.MIN_1440, 1);
        long[] old_timestamps = new long[] {baseMillis - 6 * millisInADay, baseMillis - 12 * millisInADay, baseMillis - 30 * millisInADay, baseMillis - (160* millisInADay), baseMillis - (400*millisInADay)};

        int i = 0;
        for (Granularity gran : Granularity.granularities()) {
            if (gran == Granularity.LAST) {
                break;
            }

            long from = old_timestamps[i];
            long to = baseMillis+(2 * millisInADay);

            MetricData data = httpHandler.GetDataByPoints(
                    locator.getTenantId(),
                    locator.getMetricName(),
                    from,
                    to,
                    points.get(gran));

            //The from timestamps are manufactured such that they are always before
            //the data corresponding to the granularity 'gran' has expired, it will return points for a granularity coarser
            //than 'gran'. Therefore the points returned will always be slightly less
            //than the points asked for.
            Assert.assertTrue((int) granToPoints.get(gran) > data.getData().getPoints().size());
            Assert.assertEquals(getLocatorToUnitMap().get(locator), data.getUnit());

            i++;
        }
        Assert.assertFalse(MetadataCache.getInstance().containsKey(locator, MetricMetadata.UNIT.name()));
    }

    @Test
    public void testMetricDataFetching() throws Exception {
        final Map<Granularity, Integer> points = new HashMap<Granularity, Integer>();
        points.put(Granularity.FULL, 1600);
        points.put(Granularity.MIN_5, 287);
        points.put(Granularity.MIN_20, 71);
        points.put(Granularity.MIN_60, 23);
        points.put(Granularity.MIN_240, 5);
        points.put(Granularity.MIN_1440, 1);
        for (Granularity gran : Granularity.granularities()) {
            MetricData data = httpHandler.GetDataByPoints(
                    locator.getTenantId(),
                    locator.getMetricName(),
                    baseMillis,
                    baseMillis + 86400000,
                    points.get(gran));
            Assert.assertEquals((int) granToPoints.get(gran), data.getData().getPoints().size());
            Assert.assertEquals(getLocatorToUnitMap().get(locator), data.getUnit());
        }
        Assert.assertFalse(MetadataCache.getInstance().containsKey(locator, MetricMetadata.UNIT.name()));
    }

    @Test
    public void testUnknownUnit() throws Exception {
        Locator loc = Locator.createLocatorFromPathComponents("unknown", "unit");
        MetricData data = httpHandler.GetDataByPoints(
                loc.getTenantId(),
                loc.getMetricName(),
                baseMillis,
                baseMillis + 86400000,
                1600);
        Assert.assertEquals(data.getData().getPoints().size(), 0);
        Assert.assertEquals(data.getUnit(), Util.UNKNOWN);
    }

    @Test
    public void TestHttpHappyCase() throws Exception {
        HttpGet get = new HttpGet(getMetricsQueryURI(metricName, tenantId, baseMillis));
        HttpResponse response = client.execute(get);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private URI getMetricsQueryURI(String metricName, String tenantid, long fromTimestamp) throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPortQuery).setPath("/v2.0/" + tenantid + "/views/" + metricName)
                .setParameter("from", String.valueOf(fromTimestamp))
                .setParameter("to", String.valueOf(fromTimestamp + 86400000))
                .setParameter("resolution", "full");
        return builder.build();
    }

    protected IMetric writeGaugeMetric(MetricsRW metricsRW, String name, String tenantid) throws Exception {
        final List<IMetric> metrics = new ArrayList<IMetric>();
        PreaggregatedMetric metric = getGaugeMetric(name, tenantid, System.currentTimeMillis());
        metrics.add(metric);

        metricsRW.insertMetrics(metrics);

        return metric;
    }
}
