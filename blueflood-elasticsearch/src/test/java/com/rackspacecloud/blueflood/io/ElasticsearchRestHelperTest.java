package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.utils.Metrics;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.HashMap;

import static com.rackspacecloud.blueflood.utils.Metrics.counter;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
@PrepareForTest(fullyQualifiedNames = "org.apache.http.impl.client.*")
public class ElasticsearchRestHelperTest {

  private ElasticsearchRestHelper helper;
  private HttpClientBuilder mockBuilder = mock(HttpClientBuilder.class);
  private CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
  private CloseableHttpResponse mockHttpResponse = mock(CloseableHttpResponse.class);
  private StatusLine statusLine = mock(StatusLine.class);

  @Before
  public void setup() throws IOException {
    // HttpClient dependency is nested inside pool manager build logic.
    // Mocking Http response requires mocking multiple instances of Apache http client resources.
    mockStatic(HttpClients.class);
    when(HttpClients.custom()).thenReturn(mockBuilder);
    when(mockBuilder.setConnectionManager(any())).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockHttpClient);
    when(mockHttpResponse.getStatusLine()).thenReturn(statusLine);

    resetMetricRegistry();
    helper = ElasticsearchRestHelper.getInstance();
    assertEquals(3, Metrics.getRegistry().getHistograms().size());
    // No histograms recorded yet prior to test run
    assertConnectionCounts(0);
  }

  @Test
  public void testElasticsearchGet_recordsPoolSizeMetrics() throws Exception {
    when(mockHttpClient.execute(any())).thenReturn(mockHttpResponse);
    helper.refreshIndex("foo");
    assertConnectionCounts(1);
  }

  @Test
  public void testElasticsearchPost_recordsPoolSizeMetrics() throws IOException {
    when(mockHttpClient.execute(any())).thenReturn(mockHttpResponse);
    helper.fetchEvents("fooTenant", new HashMap<>());
    assertConnectionCounts(1);
  }

  @Test
  public void testElasticsearchPostErrors_incrementsErrorCounter() throws IOException {
    // Ensure Error counter is 0 prior to failures simulation
    Assert.assertEquals(0, counter(ElasticsearchRestHelper.class, "fetchEvents Error").getCount());
    when(mockHttpClient.execute(any())).thenThrow(new IOException("test exception"));
    helper.fetchEvents("fooErrorTenant", new HashMap<>());
    Assert.assertTrue(counter(ElasticsearchRestHelper.class, "fetchEvents Error").getCount() > 0);
  }

  private void assertConnectionCounts(int expectedCount) {
    assertEquals(expectedCount, helper.getAvailablePoolSize().getCount());
    assertEquals(expectedCount, helper.getLeasedPoolSize().getCount());
    assertEquals(expectedCount, helper.getPendingPoolSize().getCount());
  }

  private void resetMetricRegistry() {
    for (String metricName : Metrics.getRegistry().getMetrics().keySet()) {
      Metrics.getRegistry().remove(metricName);
    }
  }
}
