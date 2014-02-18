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

import com.rackspacecloud.blueflood.outputs.serializers.BasicRollupsOutputSerializer;
import com.rackspacecloud.blueflood.outputs.utils.PlotRequestParser;
import junit.framework.Assert;
import org.junit.Test;

import java.util.*;

public class PlotRequestParserTest {

    @Test
    public void testSelectParams() {
        List<String> stats = new ArrayList<String>();
        stats.add("average");
        stats.add("min");
        stats.add("max");
        Set<BasicRollupsOutputSerializer.MetricStat> filters = PlotRequestParser.getStatsToFilter(stats);

        Assert.assertTrue(filters.contains(BasicRollupsOutputSerializer.MetricStat.AVERAGE));
        Assert.assertTrue(filters.contains(BasicRollupsOutputSerializer.MetricStat.MIN));
        Assert.assertTrue(filters.contains(BasicRollupsOutputSerializer.MetricStat.MAX));

        // Alternate comma delimited notation
        stats = new ArrayList<String>();
        stats.add("average,min,max");
        filters = PlotRequestParser.getStatsToFilter(stats);
        Assert.assertTrue(filters.contains(BasicRollupsOutputSerializer.MetricStat.AVERAGE));
        Assert.assertTrue(filters.contains(BasicRollupsOutputSerializer.MetricStat.MIN));
        Assert.assertTrue(filters.contains(BasicRollupsOutputSerializer.MetricStat.MAX));
    }
    
    @Test
    public void testDefaultStatsAreNotEmpty() {
        Assert.assertTrue(PlotRequestParser.DEFAULT_BASIC.size() > 0);
    }
}
