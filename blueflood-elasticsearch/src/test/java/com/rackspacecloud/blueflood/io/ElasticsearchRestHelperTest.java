package com.rackspacecloud.blueflood.io;

import com.codahale.metrics.Counter;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import static com.codahale.metrics.MetricRegistry.name;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
@PrepareForTest(fullyQualifiedNames = "org.apache.http.impl.client.*")
public class ElasticsearchRestHelperTest {

  private ElasticsearchRestHelper helper;
  private final HttpClientBuilder mockBuilder = mock(HttpClientBuilder.class);
  private final CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
  private final CloseableHttpResponse mockHttpResponse = mock(CloseableHttpResponse.class);
  private final StatusLine statusLine = mock(StatusLine.class);

  @Before
  public void setup() throws IOException {
    // HttpClient dependency is nested inside pool manager build logic.
    // Mocking Http response requires mocking multiple instances of Apache http client resources.
    // In addition, the client is only built once per instance of the class under test. If another test has already
    // initialized the static singleton, it won't have the mock in it. To work around this, be sure to obtain a new
    // instance for every test.
    mockStatic(HttpClients.class);
    when(HttpClients.custom()).thenReturn(mockBuilder);
    when(mockBuilder.setConnectionManager(any())).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockHttpClient);
    when(mockHttpResponse.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(mockHttpResponse.getEntity()).thenReturn(new StringEntity("default test response: foo bar baz"));
    when(mockHttpClient.execute(any())).thenReturn(mockHttpResponse);
    resetMetricRegistry();
    helper = ElasticsearchRestHelper.newInstanceForTests();
  }

  @Test
  public void happyPathFetch() throws Exception {
    String result = helper.fetch("banana", "monkey", "bob", Arrays.asList("q1", "q2"));
    assertThat(result, equalTo("default test response: foo bar baz"));
  }

  @Test
  public void fetchWithHttpClientException_raisesAppropriateException() throws Exception {
    IOException cause = new IOException("test exception");
    when(mockHttpClient.execute(any())).thenThrow(cause);
    try {
      helper.fetch("banana", "monkey", "bob", Arrays.asList("q1", "q2"));
      fail("Should have raised exception");
    } catch (Exception e) {
      assertThat(e, instanceOf(IOException.class));
      assertThat(e.getCause(), equalTo(cause));
      assertThat(e.getMessage(), equalTo("Unable to reach Elasticsearch"));
    }
  }

  @Test
  public void fetchWithWrongResponseCode_raisesAppropriateException() {
    when(statusLine.getStatusCode()).thenReturn(400);
    try {
      helper.fetch("banana", "monkey", "bob", Arrays.asList("q1", "q2"));
      fail("Should have raised exception");
    } catch (Exception e) {
      assertThat(e, instanceOf(IOException.class));
      assertThat(e.getMessage(), equalTo("Elasticsearch request failed"));
    }
  }

  @Test
  public void constructor_registersHttpConnectionStatsGauges() {
    // given
    resetMetricRegistry();
    Set<String> gaugesBefore = Metrics.getRegistry().getGauges().keySet();
    assertThat(gaugesBefore, empty());
    // when
    ElasticsearchRestHelper.newInstanceForTests();
    // then
    Set<String> gaugesAfter = Metrics.getRegistry().getGauges().keySet();
    assertThat(gaugesAfter, hasItem(name(ElasticsearchRestHelper.class, "Available Connections")));
    assertThat(gaugesAfter, hasItem(name(ElasticsearchRestHelper.class, "Pending Connections")));
    assertThat(gaugesAfter, hasItem(name(ElasticsearchRestHelper.class, "Leased Connections")));
  }

  @Test
  public void elasticsearchPostErrors_incrementsErrorCounter() throws IOException {
    // Ensure Error counter is 0 prior to failures simulation
    Counter fetchEventsErrorCounter = helper.getErrorCounters().get("fetchEvents");
    assertThat(fetchEventsErrorCounter.getCount(), equalTo(0L));
    when(mockHttpClient.execute(any())).thenThrow(new IOException("test exception"));
    try {
      helper.fetchEvents("fooErrorTenant", new HashMap<>());
      fail("Should have raised exception");
    } catch (Exception e) {
      assertThat(fetchEventsErrorCounter.getCount(), greaterThan(0L));
    }
  }


  private void resetMetricRegistry() {
    for (String metricName : Metrics.getRegistry().getMetrics().keySet()) {
      Metrics.getRegistry().remove(metricName);
    }
  }
}
