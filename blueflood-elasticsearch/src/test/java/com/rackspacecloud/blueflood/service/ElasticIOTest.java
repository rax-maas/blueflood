package com.rackspacecloud.blueflood.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.rackspacecloud.blueflood.io.ElasticIO;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;

public class ElasticIOTest {
    private static final int NUM_DOCS = 100;
    private static final String TENANT = "ratanasv";
    private static final String UNIT = "horse length";
    private static ElasticIO elasticIO = new ElasticIO(EmbeddedElasticSearchServer.INSTANCE);

    @BeforeClass
    public static void setup() throws IOException, InterruptedException{
//        EmbeddedElasticSearchServer.INSTANCE.getClient().prepareDeleteByQuery()
//            .setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        List<Metric> listOfMetrics = new ArrayList<Metric>();
        for (int i=0; i<NUM_DOCS; i++) {
            Locator locator = createTestLocator(i);
            Metric metric = new Metric(locator, "lobloblow", 0, new TimeValue(1, TimeUnit.DAYS), UNIT);
            listOfMetrics.add(metric);

        }
        elasticIO.insertDiscovery(listOfMetrics);
        TimeUnit.SECONDS.sleep(1);
        EmbeddedElasticSearchServer.INSTANCE.getClient().admin().indices().prepareRefresh().execute().actionGet();
    }

    @Test
    public void testWildcard() throws InterruptedException {
        List<ElasticIO.Result> result = elasticIO.search(TENANT, new ElasticIO.Discovery().withMetricName("one,two,*"));
        Assert.assertEquals(NUM_DOCS, result.size());
        System.out.println(result.toString());
        for (int i=0; i<NUM_DOCS; i++) {
            Locator locator = createTestLocator(i);
            ElasticIO.Result entry = new ElasticIO.Result(locator.getMetricName(), UNIT);
            Assert.assertTrue(result.contains(entry));
        }

        result = elasticIO.search(TENANT, new ElasticIO.Discovery().withMetricName("*,three,*"));
        Assert.assertEquals(NUM_DOCS, result.size());
        for (int i=0; i<NUM_DOCS; i++) {
            Locator locator = createTestLocator(i);
            ElasticIO.Result entry = new ElasticIO.Result(locator.getMetricName(), UNIT);
            Assert.assertTrue(result.contains(entry));
        }

        for (int i=0; i<NUM_DOCS; i++) {
            result = elasticIO.search(TENANT, new ElasticIO.Discovery().withMetricName("*" + i));
            Locator locator = createTestLocator(i);
            ElasticIO.Result entry = new ElasticIO.Result(locator.getMetricName(), UNIT);
            Assert.assertEquals(result.get(0), entry);
        }
    }


    private static Locator createTestLocator(int i) {
        return Locator.createLocatorFromPathComponents(
                TENANT, "one", "two", "three", "four", String.valueOf(i));
    }
}
