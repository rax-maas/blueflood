package com.rackspacecloud.blueflood.io;

import com.codahale.metrics.Histogram;
import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.rackspacecloud.blueflood.utils.Metrics.counter;


public class ElasticsearchRestHelperTest {
  private static EsSetup esSetup;
  protected ElasticsearchRestHelper helper;

  @BeforeClass
  public static void setupEs() {
    esSetup = new EsSetup();
  }

  @Before
  public void setup() {
    esSetup.execute(EsSetup.deleteAll());
    resetMetricRegistry();
    helper = ElasticsearchRestHelper.getInstance();
    Assert.assertEquals(3, Metrics.getRegistry().getHistograms().size());
    // No histograms recorded yet prior to test run
    assertConnectionCounts(0);
  }

  @Test
  public void testElasticsearchGet_recordsPoolSizeMetrics() throws IOException {
    helper.refreshIndex("foo");
    assertConnectionCounts(1);
  }

  @Test
  public void testElasticsearchPost_recordsPoolSizeMetrics() throws IOException {
    helper.fetchEvents("fooTenant", new HashMap<>());
    assertConnectionCounts(1);
  }

  @Test
  public void testElasticsearchPostErrors_recordsMetric() throws IOException {
    esSetup.terminate();
    Assert.assertTrue(counter(ElasticsearchRestHelper.class, "fetchEvents Error").getCount() == 0);
    helper.fetchEvents("fooTenant", new HashMap<>());
    Assert.assertTrue(counter(ElasticsearchRestHelper.class, "fetchEvents Error").getCount() > 0);
  }

  private void assertConnectionCounts(int expectedCount) {
    for (Map.Entry<String, Histogram> histogramEntry : Metrics.getRegistry().getHistograms().entrySet()) {
      Assert.assertEquals(expectedCount, histogramEntry.getValue().getCount());
    }
  }

  private void resetMetricRegistry() {
    for (String metricName : Metrics.getRegistry().getMetrics().keySet()) {
      Metrics.getRegistry().remove(metricName);
    }
  }
}
