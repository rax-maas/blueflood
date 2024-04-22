/*
 * Copyright 2024 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspacecloud.blueflood.repose

import groovy.json.JsonBuilder
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import spock.lang.IgnoreIf
import spock.lang.Shared
import spock.lang.Specification

/*
* Repose upgrade activity needs to validate various Repose filters features
* This test can be run against local vagrant or stage or prod blueflood endpoints
*
* Vagrant (local) allows us to configure repose filters to tweak as needed.
* Example: Rate limits, authorization needs
*
* Pre-requisites for running:
* Environment variables:
*  - TENANT_ID
*  - X-Auth-Token - Rackspace identity token to work with keystone-v2 filter config
*  - RATE_LIMIT_COUNT (Optional) - If this is set, we will be hitting request to this no of times to verify we reached the threshold of no of request in a minute.
*                                  This is used to test against vagrant or staging with configurable rate limit filters
*  - ENV_TO_RUN (local/stage/prod) defaults to stage
*/
class ReposeSpec extends Specification {

    final String X_AUTH_TOKEN_HEADER = "X-Auth-Token"

    @Shared
    static final String tenantId = System.getenv("TENANT_ID")
    @Shared
    static final String authToken = System.getenv("AUTH_TOKEN")
    @Shared
    static final String envToRun = System.getenv("ENV_TO_RUN")?.toLowerCase() ?: "stage"
    @Shared
    static final def defaultRateLimit = Integer.parseInt(System.getenv("RATE_LIMIT_COUNT") ?: "0")
    @Shared
    static final def httpClient = HttpClients.createDefault()
    @Shared
    String ingestEndpointUrl
    @Shared
    String queryEndpointUrl

    def setupSpec() {
        assert tenantId != null: envError("TENANT_ID")
        assert authToken != null: envError("AUTH_TOKEN")
        if (envToRun == "local") {
            ingestEndpointUrl = "http://localhost:9001/v2.0/${tenantId}"
            queryEndpointUrl = "http://localhost:9011/v2.0/${tenantId}"
        } else {
            def prefix = envToRun == "stage" ? "staging" : "global"
            ingestEndpointUrl = "https://${prefix}.metrics-ingest.api.rackspacecloud.com/v2.0/${tenantId}"
            queryEndpointUrl = "https://${prefix}.metrics.api.rackspacecloud.com/v2.0/${tenantId}"
        }
    }

    def envError(String variableName) {
        "${variableName} environment variable is missing"
    }

    def "invalid auth token returns 401"() {
        given:
        def request = getIngestHttpPostRequest()
        request.setHeader(X_AUTH_TOKEN_HEADER, "fooToken")

        when:
        def response = httpClient.execute(request)

        then:
        response.getStatusLine().getStatusCode() == 401
    }

    def "/limits returns expected static limits as configured in rate-limit filter"() {
        given:
        def url = ingestEndpointUrl + "/limits"
        def request = new HttpGet(url)
        request.addHeader(X_AUTH_TOKEN_HEADER, authToken)

        when:
        def response = httpClient.execute(request)

        then:
        response.getStatusLine().getStatusCode() == 200
    }

    def "ingesting valid metric payload succeeds"() {
        given:
        def request = getIngestHttpPostRequest()

        when:
        def response = httpClient.execute(request)

        then:
        response.getStatusLine().getStatusCode() == 200
    }

    @IgnoreIf({ defaultRateLimit == 0 })
    def "rate limited requests get rejected and return 429"() {
        when:
        def request = getIngestHttpPostRequest()
        def response = httpClient.execute(request)

        then:
        response.getStatusLine().getStatusCode() == (runIndex == defaultRateLimit ? 429 : 200)

        where:
        runIndex << (1..defaultRateLimit)
    }

    private HttpPost getIngestHttpPostRequest() {
        def url = ingestEndpointUrl + "/ingest"
        def request = new HttpPost(url)
        request.addHeader(X_AUTH_TOKEN_HEADER, authToken)
        request.addHeader("Content-Type", "application/json")
        def jsonBody = [
                [
                        "collectionTime": new Date().getTime(),
                        "ttlInSeconds"  : 172800,
                        "metricValue"   : new Random().nextInt(100),
                        "metricName"    : "repose.test.metric"
                ]
        ]
        request.setEntity(new StringEntity(new JsonBuilder(jsonBody).toString()))
        return request
    }

}
