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

package com.rackspacecloud.blueflood.io;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.service.ElasticClientManager;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EnumElasticIOTest {

    private EnumElasticIO enumElasticIO;
    private EsSetup esSetup;
    private Client esClientMock = mock(Client.class);
    private BulkRequestBuilder bulkRequestBuilderMock = mock(BulkRequestBuilder.class);
    private ListenableActionFuture listenableActionFutureMock = mock(ListenableActionFuture.class);
    private IndexRequestBuilder indexRequestBuilderMock = mock(IndexRequestBuilder.class);

    private static final String TENANT_1 = "100000";
    private static final String TENANT_2 = "200000";

    private static final String METRIC_NAME_A1 = "a.m1";
    private static final String METRIC_NAME_A2 = "a.m2";

    private static final String METRIC_NAME_B1 = "b.m1";
    private static final String METRIC_NAME_B2 = "b.m2";
    private static final String METRIC_NAME_B3 = "b.m3";

    long collectionTime = 0;
    TimeValue ttl = new TimeValue(1, TimeUnit.DAYS);

    BluefloodEnumRollup rollupWithEnumValues = new BluefloodEnumRollup()
            .withEnumValue("v1", 1L)
            .withEnumValue("v2", 1L)
            .withEnumValue("v3", 1L);
    ArrayList<String> enumValues = rollupWithEnumValues.getStringEnumValues();

    @Before
    public void setup() throws IOException {
        esSetup = new EsSetup();
        esSetup.execute(EsSetup.deleteAll());
        esSetup.execute(EsSetup.createIndex(ElasticIO.INDEX_NAME_WRITE)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping("metrics", EsSetup.fromClassPath("metrics_mapping.json")));
        esSetup.execute(EsSetup.createIndex(EnumElasticIO.ENUMS_INDEX_NAME_WRITE)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping(EnumElasticIO.ENUMS_DOCUMENT_TYPE, EsSetup.fromClassPath("metrics_mapping_enums.json")));

        enumElasticIO = new EnumElasticIO(esSetup.client());
        enumElasticIO.setINDEX_NAME_READ(EnumElasticIO.ENUMS_INDEX_NAME_WRITE);
        enumElasticIO.setINDEX_NAME_WRITE(EnumElasticIO.ENUMS_INDEX_NAME_WRITE);

        // setup mock for ElasticSearch Client
        when(bulkRequestBuilderMock.execute()).thenReturn(listenableActionFutureMock);
        when(esClientMock.prepareBulk()).thenReturn(bulkRequestBuilderMock);
        when(esClientMock.prepareIndex(EnumElasticIO.ENUMS_INDEX_NAME_WRITE, EnumElasticIO.ENUMS_DOCUMENT_TYPE))
                .thenReturn(indexRequestBuilderMock);
        when(indexRequestBuilderMock.setId(anyString())).thenReturn(indexRequestBuilderMock);
        when(indexRequestBuilderMock.setSource(any(XContentBuilder.class))).thenReturn(indexRequestBuilderMock);
        when(indexRequestBuilderMock.setCreate(anyBoolean())).thenReturn(indexRequestBuilderMock);
        when(indexRequestBuilderMock.setRouting(anyString())).thenReturn(indexRequestBuilderMock);
    }

    @After
    public void tearDown() {
        esSetup.terminate();
    }

    @Test
    public void testCreateEnumElasticIO() {
        EnumElasticIO enumElasticIO1 = new EnumElasticIO();
        Assert.assertNotNull("create EnumElasticIO() failed", enumElasticIO1);

        EnumElasticIO enumElasticIO2 = new EnumElasticIO(esClientMock);
        Assert.assertNotNull("create EnumElasticIO(client) failed", enumElasticIO2);

        ElasticClientManager managerMock = mock(ElasticClientManager.class);
        EnumElasticIO enumElasticIO3 = new EnumElasticIO(managerMock);
        Assert.assertNotNull("create EnumElasticIO(manager) failed", enumElasticIO3);
        verify(managerMock, times(1)).getClient();
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateSingleRequest_WithNullMetricName() throws IOException {
        Discovery discovery = new Discovery(TENANT_1, null);
        enumElasticIO.createSingleRequest(discovery);
    }

    @Test
    public void testCreateSingleRequest() throws IOException {
        Discovery discovery = new Discovery(TENANT_1, METRIC_NAME_A1);
        IndexRequestBuilder builder = enumElasticIO.createSingleRequest(discovery);
        Assert.assertNotNull(builder);
        Assert.assertEquals(TENANT_1 + ":" + METRIC_NAME_A1, builder.request().id());
        final String expectedIndex =
                "index {" +
                    "[" + EnumElasticIO.ENUMS_INDEX_NAME_WRITE + "]" +
                    "[" + EnumElasticIO.ENUMS_DOCUMENT_TYPE + "]" +
                    "["+ TENANT_1 + ":" + METRIC_NAME_A1 + "], " +
                    "source[{" +
                        "\"tenantId\":\"" + TENANT_1 + "\"," +
                        "\"metric_name\":\"" + METRIC_NAME_A1 + "\"" +
                    "}]}";
        Assert.assertEquals(expectedIndex, builder.request().toString());
        Assert.assertEquals(builder.request().routing(), TENANT_1);
    }

    @Test
    public void testInsertDiscoveryWithEmptyBatch() throws IOException {
        when(esClientMock.prepareBulk()).thenReturn(bulkRequestBuilderMock);
        enumElasticIO.setClient(esClientMock);

        // create empty metrics list and call insertDiscovery with it
        List<IMetric> metrics = new ArrayList<IMetric>();
        enumElasticIO.insertDiscovery(metrics);

        // verify nothing is executed
        verify(esClientMock, never()).prepareBulk();
        verify(bulkRequestBuilderMock, never()).execute();
    }

    @Test
    public void testInsertDiscoveryWithInvalidMetricClass() throws IOException {
        ListenableActionFuture listenableActionFutureMock = mock(ListenableActionFuture.class);
        when(esClientMock.prepareBulk()).thenReturn(bulkRequestBuilderMock);
        when(bulkRequestBuilderMock.execute()).thenReturn(listenableActionFutureMock);
        enumElasticIO.setClient(esClientMock);

        // create empty metrics list and call insertDiscovery with it
        List<IMetric> metrics = new ArrayList<IMetric>();
        Locator locator = Locator.createLocatorFromPathComponents(TENANT_1, METRIC_NAME_A1);
        metrics.add(new Metric(locator, "", 0L, new TimeValue(1, TimeUnit.DAYS), null));
        enumElasticIO.insertDiscovery(metrics);

        // verify
        verify(bulkRequestBuilderMock, never()).add(any(IndexRequestBuilder.class));
    }

    @Test
    public void testInsertDiscoverySingle() throws IOException {
        enumElasticIO.setClient(esClientMock);

        // test single metric insert
        Locator locator = Locator.createLocatorFromPathComponents(TENANT_1, METRIC_NAME_A1);
        IMetric metric = new PreaggregatedMetric(collectionTime, locator, ttl, rollupWithEnumValues);
        enumElasticIO.insertDiscovery(metric);
        // verify
        verify(esClientMock, times(1)).prepareIndex(EnumElasticIO.ENUMS_INDEX_NAME_WRITE, EnumElasticIO.ENUMS_DOCUMENT_TYPE);
        verify(indexRequestBuilderMock).setId(TENANT_1 + ":" + METRIC_NAME_A1);
        verify(bulkRequestBuilderMock, times(1)).execute();
    }

    @Test
    public void testInsertDiscoveryBatch() throws IOException {
        enumElasticIO.setClient(esClientMock);

        // create PreaggregatedMetric metrics list and call insertDiscovery with it
        List<IMetric> metrics = new ArrayList<IMetric>();
        Locator locator1 = Locator.createLocatorFromPathComponents(TENANT_1, METRIC_NAME_A1);
        Locator locator2 = Locator.createLocatorFromPathComponents(TENANT_2, METRIC_NAME_B1);
        metrics.add(new PreaggregatedMetric(collectionTime, locator1, ttl, rollupWithEnumValues));
        metrics.add(new PreaggregatedMetric(collectionTime, locator2, ttl, rollupWithEnumValues));
        enumElasticIO.insertDiscovery(metrics);

        // verify client execute with right params
        verify(esClientMock, times(2)).prepareIndex(EnumElasticIO.ENUMS_INDEX_NAME_WRITE, EnumElasticIO.ENUMS_DOCUMENT_TYPE);
        verify(indexRequestBuilderMock, times(2)).setId(anyString());
        verify(indexRequestBuilderMock).setId(TENANT_1 + ":" + METRIC_NAME_A1);
        verify(indexRequestBuilderMock).setId(TENANT_2 + ":" + METRIC_NAME_B1);
        verify(indexRequestBuilderMock, times(2)).setSource(any(XContentBuilder.class));
        verify(indexRequestBuilderMock, times(2)).setRouting(anyString());
        verify(indexRequestBuilderMock).setRouting(TENANT_1);
        verify(indexRequestBuilderMock).setRouting(TENANT_2);
        verify(bulkRequestBuilderMock, times(1)).execute();
    }

    @Test
    public void testSearchWithNoWildCard() throws Exception {

        createTestMetrics();

        List<SearchResult> results = enumElasticIO.search(TENANT_1, METRIC_NAME_A1);
        Assert.assertEquals("results should have 1 result", 1, results.size());

        // get the result
        SearchResult result = results.get(0);
        Assert.assertEquals(TENANT_1, result.getTenantId());
        Assert.assertEquals(METRIC_NAME_A1, result.getMetricName());
        Assert.assertNotNull(result.getEnumValues());
        Assert.assertTrue(result.getEnumValues().equals(enumValues));
    }

    @Test
    public void testSearchWithWildCard() throws Exception {

        createTestMetrics();

        // tenant1 expected search results
        SearchResult tenant1Entry1 =  new SearchResult(TENANT_1, METRIC_NAME_A1, null, enumValues);
        SearchResult tenant1Entry2 =  new SearchResult(TENANT_1, METRIC_NAME_A2, null, enumValues);
        SearchResult tenant1Entry3 =  new SearchResult(TENANT_1, METRIC_NAME_B1, null, enumValues);
        SearchResult tenant1Entry4 =  new SearchResult(TENANT_1, METRIC_NAME_B2, null, enumValues);

        // tenant2 expected search results
        SearchResult tenant2Entry1 =  new SearchResult(TENANT_2, METRIC_NAME_B1, null, enumValues);
        SearchResult tenant2Entry2 =  new SearchResult(TENANT_2, METRIC_NAME_B2, null, enumValues);
        SearchResult tenant2Entry3 =  new SearchResult(TENANT_2, METRIC_NAME_B3, null, enumValues);

        // results1 = search all tenant1
        List<SearchResult> results1 = enumElasticIO.search(TENANT_1, "*");
        Assert.assertEquals("results1 should have 4 results", 4, results1.size());

        // results2 = search all tenant2
        List<SearchResult> results2 = enumElasticIO.search(TENANT_2, "*");
        Assert.assertEquals("results2 should have 3 results", 3, results2.size());

        // assert results1
        Assert.assertTrue("results1 should contain tenant1 entry1", results1.contains(tenant1Entry1));
        Assert.assertTrue("results1 should contain tenant1 entry2", results1.contains(tenant1Entry2));
        Assert.assertTrue("results1 should contain tenant1 entry3", results1.contains(tenant1Entry3));
        Assert.assertTrue("results1 should contain tenant1 entry4", results1.contains(tenant1Entry4));
        Assert.assertTrue("results1 should not contain tenant2 entry1", !results1.contains(tenant2Entry1));
        Assert.assertTrue("results1 should not contain tenant2 entry2", !results1.contains(tenant2Entry2));
        Assert.assertTrue("results1 should not contain tenant2 entry3", !results1.contains(tenant2Entry3));

        // assert results2
        Assert.assertTrue("results2 should contain tenant2 entry1", results2.contains(tenant2Entry1));
        Assert.assertTrue("results2 should contain tenant2 entry2", results2.contains(tenant2Entry2));
        Assert.assertTrue("results2 should contain tenant2 entry3", results2.contains(tenant2Entry3));
        Assert.assertTrue("results2 should not contain tenant1 entry1", !results2.contains(tenant1Entry1));
        Assert.assertTrue("results2 should not contain tenant1 entry2", !results2.contains(tenant1Entry2));

        // results3 = search *.m1 for tenant1
        List<SearchResult> results3 = enumElasticIO.search(TENANT_1, "*.m1");
        Assert.assertEquals("results3 should have 2 results", 2, results3.size());
        Assert.assertTrue("results3 should contain tenant1 entry1", results3.contains(tenant1Entry1));
        Assert.assertTrue("results3 should contain tenant1 entry3", results3.contains(tenant1Entry3));
        Assert.assertTrue("results3 should not contain tenant1 entry2", !results3.contains(tenant1Entry2));
        Assert.assertTrue("results3 should not contain tenant1 entry4", !results3.contains(tenant1Entry4));
        Assert.assertTrue("results3 should not contain tenant2 entry1", !results3.contains(tenant2Entry1));
        Assert.assertTrue("results3 should not contain tenant2 entry2", !results3.contains(tenant2Entry2));
        Assert.assertTrue("results3 should not contain tenant2 entry3", !results3.contains(tenant2Entry3));
    }

    @Test
    public void testBatchQueryWithWildCards() throws Exception {

        createTestMetrics();

        String tenantId = TENANT_1;
        String query1 = "a.*";
        String query2 = "*.m1";
        List<SearchResult> results;
        ArrayList<String> queries = new ArrayList<String>();
        queries.add(query1);
        queries.add(query2);
        results = enumElasticIO.search(tenantId, queries);

        // query1 will return 2 results, query2 will return 2 results, but we get back 3 because of intersection
        Assert.assertEquals("results should have 3 results", results.size(), 3);

        SearchResult result1 =  new SearchResult(TENANT_1, METRIC_NAME_A1, null, enumValues);
        SearchResult result2 =  new SearchResult(TENANT_1, METRIC_NAME_A2, null, enumValues);
        SearchResult result3 =  new SearchResult(TENANT_1, METRIC_NAME_B1, null, enumValues);

        Assert.assertTrue("results should contain result1", results.contains(result1));
        Assert.assertTrue("results should contain result2", results.contains(result2));
        Assert.assertTrue("results should contain result3", results.contains(result3));
    }

    private IMetric createTestMetric(String tenantId, String metricName) {
        Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricName);
        return new PreaggregatedMetric(0, locator, new TimeValue(1, TimeUnit.DAYS), rollupWithEnumValues);
    }

    private void createTestMetrics() throws IOException {
        List<IMetric> metrics = new ArrayList<IMetric>();

        // add TENANT_1 metrics
        metrics.add(createTestMetric(TENANT_1, METRIC_NAME_A1));
        metrics.add(createTestMetric(TENANT_1, METRIC_NAME_A2));
        metrics.add(createTestMetric(TENANT_1, METRIC_NAME_B1));
        metrics.add(createTestMetric(TENANT_1, METRIC_NAME_B2));

        // add TENANT_2 metrics
        metrics.add(createTestMetric(TENANT_2, METRIC_NAME_B1));
        metrics.add(createTestMetric(TENANT_2, METRIC_NAME_B2));
        metrics.add(createTestMetric(TENANT_2, METRIC_NAME_B3));
        enumElasticIO.insertDiscovery(metrics);
        esSetup.client().admin().indices().prepareRefresh().execute().actionGet();
    }
}
