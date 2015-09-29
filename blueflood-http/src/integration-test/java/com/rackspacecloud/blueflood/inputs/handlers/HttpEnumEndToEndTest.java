/*
 * Copyright 2013-2015 Rackspace
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
package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.http.HttpClientVendor;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.outputs.handlers.HttpMetricDataQueryServer;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.MetricMetadata;
import com.rackspacecloud.blueflood.types.RollupType;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


public class HttpEnumEndToEndTest extends IntegrationTestBase{
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;
    private static int queryPort;
    private static HttpQueryService httpQueryService;
    private static final String tenant_id = "333333";
    IMetric metric;
    IMetric metric1;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        queryPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT);
        httpQueryService = new HttpQueryService();
        HttpMetricDataQueryServer queryServer = new HttpMetricDataQueryServer();
        httpQueryService.setServer(queryServer);
        httpQueryService.startService();

        vendor = new HttpClientVendor();
        client = vendor.getClient();

        metric = writeEnumMetric("enum_metric2");
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), null);
        MetadataCache.getInstance().put(metric.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.ENUM.toString());

        metric1 = writeEnumMetric("enum_metric3");
        MetadataCache.getInstance().put(metric1.getLocator(), MetricMetadata.TYPE.name().toLowerCase(), null);
        MetadataCache.getInstance().put(metric1.getLocator(), MetricMetadata.ROLLUP_TYPE.name().toLowerCase(), RollupType.ENUM.toString());
    }

    @Test
    public void testEnumEndToEndHappyCase() throws Exception {
        Map <String, String> parameterMap = new HashMap<String, String>();
        parameterMap.put("from", String.valueOf(metric.getCollectionTime() - 10000));
        parameterMap.put("to", String.valueOf(metric1.getCollectionTime() + 10000));
        parameterMap.put("points", "200");

        HttpGet get = new HttpGet(getEnumQueryURI("enum_metric2", parameterMap));
        HttpResponse response = client.execute(get);

        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertTrue(responseString.contains("enumValue"));
    }

    private URI getEnumQueryURI(String metricName, Map<String, String> parameterMap) throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v2.0/" + tenant_id + "/views/"+metricName);

        Set<String> parameters = parameterMap.keySet();
        Iterator<String> setIterator = parameters.iterator();
        while (setIterator.hasNext()){
            String paramName = setIterator.next();
            builder.setParameter(paramName, parameterMap.get(paramName));
        }
        return builder.build();
    }

    @AfterClass
    public static void tearDownClass() throws Exception{
        httpQueryService.stopService();
    }
}
