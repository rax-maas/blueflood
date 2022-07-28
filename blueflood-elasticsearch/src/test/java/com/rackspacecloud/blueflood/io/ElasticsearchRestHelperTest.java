package com.rackspacecloud.blueflood.io;

import com.codahale.metrics.Counter;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.equalTo;
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
    assertThat(registeredGauges, hasItem(name(ElasticsearchRestHelper.class, "Available Connections")));
    assertThat(registeredGauges, hasItem(name(ElasticsearchRestHelper.class, "Pending Connections")));
    assertThat(registeredGauges, hasItem(name(ElasticsearchRestHelper.class, "Leased Connections")));
  }

  @Test
  public void testElasticsearchPostErrors_incrementsErrorCounter() throws IOException {
    // Ensure Error counter is 0 prior to failures simulation
    helper = ElasticsearchRestHelper.getInstance();
    Counter fetchEventsErrorCounter = helper.getErrorCounters().get("fetchEvents");
    assertThat(fetchEventsErrorCounter.getCount(), equalTo(0L));
    when(mockHttpClient.execute(any())).thenThrow(new IOException("test exception"));
    helper.fetchEvents("fooErrorTenant", new HashMap<>());
    assertThat(fetchEventsErrorCounter.getCount(), greaterThan(0L));
  }


  private void resetMetricRegistry() {
    for (String metricName : Metrics.getRegistry().getMetrics().keySet()) {
      Metrics.getRegistry().remove(metricName);
    }
  }
}
