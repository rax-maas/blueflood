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

package com.rackspacecloud.blueflood.http;

import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class HttpRequestWithDecodedQueryParamsTest {

    @Test
    public void testQueryParamsDecode() {
        final DefaultHttpRequest defaultRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "http://localhost/v1.0/ac98760XYZ/experimental/metric_views/metXYZ?from=12345&to=56789&points=100"
                + "&foo=x,y,z&foo=p");
        final HTTPRequestWithDecodedQueryParams requestWithParams =
                HTTPRequestWithDecodedQueryParams.createHttpRequestWithDecodedQueryParams(defaultRequest);

        Map<String, List<String>> queryParams = requestWithParams.getQueryParams();
        Assert.assertEquals(4, queryParams.size());
        final String fromParam = queryParams.get("from").get(0);
        final String toParam = queryParams.get("to").get(0);
        final String pointsParam = queryParams.get("points").get(0);
        List<String> fooParams = queryParams.get("foo");

        Assert.assertEquals(12345, Integer.parseInt(fromParam));
        Assert.assertEquals(56789, Integer.parseInt(toParam));
        Assert.assertEquals(100, Integer.parseInt(pointsParam));
        Assert.assertEquals(2, fooParams.size());

        for (String fooParam : fooParams) {
            Assert.assertTrue(fooParam.equals("x,y,z") || fooParam.equals("p"));
        }
    }
}
