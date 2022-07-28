package com.rackspacecloud.blueflood.io;

import com.codahale.metrics.Counter;
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
import java.util.Set;

import static com.codahale.metrics.MetricRegistry.name;
import static org.junit.Assert.assertTrue;
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
  }

  @Test
  public void testConstructor_registersHttpConnectionStatsGauges() {
    // when
    helper = ElasticsearchRestHelper.getInstance();
    // then
    Set<String> registeredGauges = Metrics.getRegistry().getGauges().keySet();
    assertTrue(registeredGauges.contains(name(ElasticsearchRestHelper.class, "Available Connections")));
    assertTrue(registeredGauges.contains(name(ElasticsearchRestHelper.class, "Pending Connections")));
    assertTrue(registeredGauges.contains(name(ElasticsearchRestHelper.class, "Leased Connections")));
  }

  @Test
  public void testElasticsearchPostErrors_incrementsErrorCounter() throws IOException {
    // Ensure Error counter is 0 prior to failures simulation
    helper = ElasticsearchRestHelper.getInstance();
    Counter fetchEventsErrorCounter = helper.getErrorCounters().get("fetchEvents");
    Assert.assertEquals(0, fetchEventsErrorCounter.getCount());
    when(mockHttpClient.execute(any())).thenThrow(new IOException("test exception"));
    helper.fetchEvents("fooErrorTenant", new HashMap<>());
    assertTrue(fetchEventsErrorCounter.getCount() > 0);
  }


  private void resetMetricRegistry() {
    for (String metricName : Metrics.getRegistry().getMetrics().keySet()) {
      Metrics.getRegistry().remove(metricName);
    }
  }
}
