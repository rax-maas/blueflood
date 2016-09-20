/*
 * Copyright 2014 Rackspace
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

package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.exceptions.InvalidRequestException;
import com.rackspacecloud.blueflood.outputs.serializers.BasicRollupsOutputSerializer;
import com.rackspacecloud.blueflood.outputs.utils.PlotRequestParser;
import junit.framework.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

import static org.hamcrest.core.StringStartsWith.startsWith;

public class PlotRequestParserTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testSelectParams() {
        List<String> stats = new ArrayList<String>();
        stats.add("average");
        stats.add("min");
        stats.add("max");
        stats.add("sum");
        Set<BasicRollupsOutputSerializer.MetricStat> filters = PlotRequestParser.getStatsToFilter(stats);

        Assert.assertTrue(filters.contains(BasicRollupsOutputSerializer.MetricStat.AVERAGE));
        Assert.assertTrue(filters.contains(BasicRollupsOutputSerializer.MetricStat.MIN));
        Assert.assertTrue(filters.contains(BasicRollupsOutputSerializer.MetricStat.MAX));

        // Alternate comma delimited notation
        stats = new ArrayList<String>();
        stats.add("average,min,max,sum");
        filters = PlotRequestParser.getStatsToFilter(stats);
        Assert.assertTrue(filters.contains(BasicRollupsOutputSerializer.MetricStat.AVERAGE));
        Assert.assertTrue(filters.contains(BasicRollupsOutputSerializer.MetricStat.MIN));
        Assert.assertTrue(filters.contains(BasicRollupsOutputSerializer.MetricStat.MAX));
        Assert.assertTrue(filters.contains(BasicRollupsOutputSerializer.MetricStat.SUM));
    }
    
    @Test
    public void testDefaultStatsAreNotEmpty() {
        Assert.assertTrue(PlotRequestParser.DEFAULT_BASIC.size() > 0);
    }

    @Test
    public void testNoParamsForViewsRequest() throws InvalidRequestException {
        expectedEx.expect(InvalidRequestException.class);
        expectedEx.expectMessage("No query parameters present.");

        PlotRequestParser.parseParams(new HashMap<String, List<String>>());
    }

    @Test
    public void testWithNoPointsAndResolutionParamsForViewsRequest() throws InvalidRequestException {
        expectedEx.expect(InvalidRequestException.class);
        expectedEx.expectMessage("Either 'points' or 'resolution' is required.");


        HashMap<String, List<String>> params = new HashMap<String, List<String>>();
        params.put("from", Arrays.asList("100"));

        PlotRequestParser.parseParams(params);
    }

    @Test
    public void testWithInvalidPointsParamsForViewsRequest() throws InvalidRequestException {
        expectedEx.expect(InvalidRequestException.class);
        expectedEx.expectMessage(startsWith("Invalid parameter: points="));


        HashMap<String, List<String>> params = new HashMap<String, List<String>>();
        params.put("points", new ArrayList<String>());

        PlotRequestParser.parseParams(params);
    }

    @Test
    public void testWithInvalidResolutionParamsForViewsRequest() throws InvalidRequestException {
        expectedEx.expect(InvalidRequestException.class);
        expectedEx.expectMessage(startsWith("Invalid parameter: resolution="));

        HashMap<String, List<String>> params = new HashMap<String, List<String>>();
        params.put("resolution", new ArrayList<String>());

        PlotRequestParser.parseParams(params);
    }

    @Test
    public void testWithInvalidFromParamsForViewsRequest() throws InvalidRequestException {
        expectedEx.expect(InvalidRequestException.class);
        expectedEx.expectMessage(startsWith("Invalid parameter: from="));

        HashMap<String, List<String>> params = new HashMap<String, List<String>>();
        params.put("from", new ArrayList<String>());
        params.put("points", Arrays.asList("1"));

        PlotRequestParser.parseParams(params);
    }

    @Test
    public void testWithInvalidToParamsForViewsRequest() throws InvalidRequestException {
        expectedEx.expect(InvalidRequestException.class);
        expectedEx.expectMessage(startsWith("Invalid parameter: to="));

        HashMap<String, List<String>> params = new HashMap<String, List<String>>();
        params.put("from", Arrays.asList("100"));
        params.put("to", new ArrayList<String>());
        params.put("points", Arrays.asList("1"));

        PlotRequestParser.parseParams(params);
    }

    @Test
    public void testWithInvalidFromValueParamsForViewsRequest() throws InvalidRequestException {
        expectedEx.expect(InvalidRequestException.class);
        expectedEx.expectMessage("parameter 'from' must be a valid long");

        HashMap<String, List<String>> params = new HashMap<String, List<String>>();
        params.put("from", Arrays.asList("xxx"));
        params.put("to", Arrays.asList("1"));
        params.put("points", Arrays.asList("1"));

        PlotRequestParser.parseParams(params);
    }

    @Test
    public void testWithInvalidToValueParamsForViewsRequest() throws InvalidRequestException {
        expectedEx.expect(InvalidRequestException.class);
        expectedEx.expectMessage("parameter 'to' must be a valid long");

        HashMap<String, List<String>> params = new HashMap<String, List<String>>();
        params.put("from", Arrays.asList("1"));
        params.put("to", Arrays.asList("xxx"));
        params.put("points", Arrays.asList("1"));

        PlotRequestParser.parseParams(params);
    }

    @Test
    public void testWithToGreaterThanFromParamsForViewsRequest() throws InvalidRequestException {
        expectedEx.expect(InvalidRequestException.class);
        expectedEx.expectMessage("parameter 'to' must be greater than 'from'");

        HashMap<String, List<String>> params = new HashMap<String, List<String>>();
        params.put("from", Arrays.asList("2"));
        params.put("to", Arrays.asList("1"));
        params.put("points", Arrays.asList("1"));

        PlotRequestParser.parseParams(params);
    }

    @Test
    public void testWithInvalidPointsValueParamsForViewsRequest() throws InvalidRequestException {
        expectedEx.expect(InvalidRequestException.class);
        expectedEx.expectMessage("parameter 'points' must be a valid integer");

        HashMap<String, List<String>> params = new HashMap<String, List<String>>();
        params.put("from", Arrays.asList("1"));
        params.put("to", Arrays.asList("2"));
        params.put("points", Arrays.asList("xxx"));

        PlotRequestParser.parseParams(params);
    }

    @Test
    public void testWithInvalidResolutionValueParamsForViewsRequest() throws InvalidRequestException {
        expectedEx.expect(InvalidRequestException.class);
        expectedEx.expectMessage("parameter 'resolution' is not valid. Allowed values ['FULL', 'MIN5', 'MIN20', 'MIN60', 'MIN240', 'MIN1440']");

        HashMap<String, List<String>> params = new HashMap<String, List<String>>();
        params.put("from", Arrays.asList("1"));
        params.put("to", Arrays.asList("2"));
        params.put("resolution", Arrays.asList("xxx"));

        PlotRequestParser.parseParams(params);
    }
}
