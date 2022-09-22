package com.rackspacecloud.blueflood

import com.fasterxml.jackson.databind.ObjectMapper
import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase
import com.rackspacecloud.blueflood.io.datastax.DCassandraUtilsIO
import com.rackspacecloud.blueflood.utils.CassandraTestServer
import com.rackspacecloud.blueflood.utils.ElasticsearchTestServer
import com.rackspacecloud.blueflood.utils.TestServers
import groovy.json.JsonSlurper
import org.apache.http.HttpResponse
import org.apache.http.util.EntityUtils
import org.junit.*
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Specification

import java.util.*
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*
import static org.testcontainers.shaded.org.awaitility.Awaitility.await

//@Ignore("because this is a wip")
class IngestQuerySpec extends Specification {
//     Can't extend this because it causes the db code to initialize, and the db may not be up yet.
    private static HttpIntegrationTestBase httpIntegrationTestBase
    private ObjectMapper mapper = new ObjectMapper()

    def setupSpec() {
//        TestServers.ensureStarted()
        HttpIntegrationTestBase.setUpHttp()
        httpIntegrationTestBase = new HttpIntegrationTestBase()
    }

    @AfterClass
    static void stopThings() {
        HttpIntegrationTestBase.shutdown()
    }

    def 'i can ingest and retrieve one metric'() {
        given:
        long start = System.currentTimeMillis()    'GET /view/com.example.foo.bar/tags/{dc:ord1,type:web}'
        def metric = [
            metricName: "banana",
            tags: [
                    "dc": "ord1",
                    "type": "web",
                    "color": "red"
            ],
            unit: "monkeys",
            collectionTime: start,
            metricValue: 13579,
            ttlInSeconds: 10
        ]
        String inputBody = mapper.writeValueAsString([metric])

        when:
        HttpResponse createResponse = httpIntegrationTestBase.httpPost(
                "13579", "/v2.0/13579/ingest", inputBody)
        EntityUtils.consume(createResponse.getEntity())

        then:
        createResponse.statusLine.statusCode == 200

        when:
        HttpResponse queryResponse = httpIntegrationTestBase.querySingleplot(
                "13579", "com.example.metric.one.banana", start, System.currentTimeMillis(),
                "", "full", "")

        then:
        queryResponse.statusLine.statusCode == 200
        def retrievedData = new JsonSlurper().parse(queryResponse.getEntity().content)
        retrievedData == [
            metadata: [
                marker: null,
                count: 1,
                limit: null,
                next_href: null
            ],
            unit: "monkeys",
            values: [
                [numPoints: 1, average: 13579, timestamp: start]
            ]
        ]
    }

    def 'i can ingest and retrieve one tagged metric'()  {
        long start = System.currentTimeMillis()
        Map<String, Object> metric = new HashMap<String, Object>() {{
            put("metricName", "bananas")
            put("unit", "bunch")
            put("collectionTime", start)
            put("metricValue", 24680)
            put("ttlInSeconds", 10)
            put("tags", new HashMap<String, String>() {{
                put("name", "bob")
                put("pet", "monkey")
            }})
        }}
        List<Map<String, Object>> metricList = Collections.singletonList(metric)
        String json = mapper.writeValueAsString(metricList)

        HttpResponse createResponse = httpIntegrationTestBase.httpPost("13579", "/v2.0/13579/ingest", json)
        EntityUtils.consume(createResponse.getEntity())
        assertThat(createResponse.getStatusLine().getStatusCode(), equalTo(200))
//        pause("where is the tagged metric now?")
        // todo: this is the wrong query. can't query by name for tagged metrics. have to know the metric uid!
        HttpResponse queryResponse = httpIntegrationTestBase.querySingleplot("13579", "bananas",
                start, System.currentTimeMillis(), "", "full", "")
        assertThat(createResponse.getStatusLine().getStatusCode(), equalTo(200))
        Map<?, ?> retrievedData = mapper.readValue(EntityUtils.toString(queryResponse.getEntity()), Map.class)

        expect:
        def expected = new HashMap<String, Object>() {{
            put("metadata", new HashMap<String, Object>() {{
                put("marker", null)
                put("count", 1)
                put("limit", null)
                put("next_href", null)
            }})
            put("unit", "bunch")
            put("values", Collections.singletonList(new HashMap<String, Object>() {{
                put("numPoints", 1)
                put("average", 24680)
                put("timestamp", start)
            }}))
        }}
        retrievedData == expected
    }
}
