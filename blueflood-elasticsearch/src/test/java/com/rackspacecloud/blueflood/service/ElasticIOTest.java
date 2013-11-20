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

package com.rackspacecloud.blueflood.service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import com.rackspacecloud.blueflood.io.ElasticIO;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;

public class ElasticIOTest {
    private static final int NUM_PARENT_ELEMENTS = 30;
    private static final List<String> CHILD_ELEMENTS = Arrays.asList("A", "B", "C");
    private static final int NUM_GRANDCHILD_ELEMENTS = 3;
    private static final int NUM_DOCS = NUM_PARENT_ELEMENTS * CHILD_ELEMENTS.size() * NUM_GRANDCHILD_ELEMENTS;
    private static final String TENANT_A = "ratanasv";
    private static final String TENANT_B = "someotherguy";
    private static final String UNIT = "horse length";
    private static final Map<String, List<Locator>> locatorMap = new HashMap<String, List<Locator>>();
    private static ElasticIO elasticIO = new ElasticIO(EmbeddedElasticSearchServer.getInstance());

    private static ElasticIO.Result createExpectedResult(String tenantId, int x, String y, int z) {
        Locator locator = createTestLocator(tenantId, x, y, z);
        return new ElasticIO.Result(tenantId, locator.getMetricName(), UNIT);
    }
    private static Locator createTestLocator(String tenantId, int x, String y, int z) {
        String xs = (x < 10 ? "0" : "") + String.valueOf(x);
        return Locator.createLocatorFromPathComponents(
                tenantId, "one", "two", "three" + xs,
                "four" + y,
                "five" + String.valueOf(z));
    }

    private static List<Locator> createComplexTestLocators(String tenantId) {
        Locator locator;
        List<Locator> locators = new ArrayList<Locator>();
        locatorMap.put(tenantId, locators);
        for (int x = 0; x < NUM_PARENT_ELEMENTS; x++) {
            for (String y : CHILD_ELEMENTS) {
                for (int z = 0; z < NUM_GRANDCHILD_ELEMENTS; z++) {
                    locator = createTestLocator(tenantId, x, y, z);
                    locators.add(locator);
                }
            }
        }
        return locators;
    }

    private static List<Metric> createTestMetrics(String tenantId) {
        Metric metric;
        List<Metric> metrics = new ArrayList<Metric>();
        List<Locator> locators = createComplexTestLocators(tenantId);
        for (Locator locator : locators) {
            metric = new Metric(locator, "blarg", 0, new TimeValue(1, TimeUnit.DAYS), UNIT);
            metrics.add(metric);
        }
        return metrics;
    }

    @BeforeClass
    public static void setup() throws IOException, InterruptedException{
        IndicesAdminClient iac = EmbeddedElasticSearchServer.getInstance().getClient().admin().indices();

        XContentBuilder mapping =  XContentFactory.jsonBuilder().startObject()
            .startObject("metrics")
                .startObject("properties")
                    .startObject("TENANT_ID")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("TYPE")
                    .field("type", "string")
                    .field("index", "not_analyzed")
                    .endObject()
                    .startObject("UNIT")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("METRIC_NAME")
                        .field("type", "multi_field")
                        .startObject("fields")
                            .startObject("METRIC_NAME")
                                .field("type", "string")
                                .field("index", "analyzed")
                            .endObject()
                            .startObject("RAW_METRIC_NAME")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
        .endObject();


        PutMappingRequestBuilder pmrb = new PutMappingRequestBuilder(iac);
        pmrb.setType("metrics")
                .setSource(mapping)
                .execute()
                .actionGet();

        elasticIO.insertDiscovery(createTestMetrics(TENANT_A));
        elasticIO.insertDiscovery(createTestMetrics(TENANT_B));

        iac.prepareRefresh().execute().actionGet();
    }

    @Test
    public void testNoCrossTenantResults() {
        List<ElasticIO.Result> results = elasticIO.search(new ElasticIO.Discovery(TENANT_A, "*"));
        Assert.assertEquals(NUM_DOCS, results.size());
        for (ElasticIO.Result result : results) {
            Assert.assertNotNull(result.getTenantId());
            Assert.assertNotSame(TENANT_B, result.getTenantId());
        }
    }

    @Test
    public void testWildcard() {
        ElasticIO.Result entry;
        List<ElasticIO.Result> results;
        results = elasticIO.search(new ElasticIO.Discovery(TENANT_A, "one.two.*"));
        List<Locator> locators = locatorMap.get(TENANT_A);
        Assert.assertEquals(locators.size(), results.size());
        for (Locator locator : locators) {
            entry =  new ElasticIO.Result(TENANT_A, locator.getMetricName(), UNIT);
            Assert.assertTrue((results.contains(entry)));
        }

        results = elasticIO.search(new ElasticIO.Discovery(TENANT_A, "*.fourA.*"));
        Assert.assertEquals(NUM_PARENT_ELEMENTS * NUM_GRANDCHILD_ELEMENTS, results.size());
        for (int x = 0; x < NUM_PARENT_ELEMENTS; x++) {
            for (int z = 0; z < NUM_GRANDCHILD_ELEMENTS; z++) {
                entry = createExpectedResult(TENANT_A, x, "A", z);
                Assert.assertTrue(results.contains(entry));
            }
        }

        results = elasticIO.search(new ElasticIO.Discovery(TENANT_A, "*.three1*.four*.five2"));
        Assert.assertEquals(10 * CHILD_ELEMENTS.size(), results.size());
        for (int x = 10; x < 20; x++) {
            for (String y : CHILD_ELEMENTS) {
                entry = createExpectedResult(TENANT_A, x, y, 2);
                Assert.assertTrue(results.contains(entry));
            }
        }
    }
}
