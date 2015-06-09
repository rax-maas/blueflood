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

package com.rackspacecloud.blueflood.outputs.utils;

import com.rackspacecloud.blueflood.exceptions.InvalidRequestException;
import com.rackspacecloud.blueflood.outputs.serializers.BasicRollupsOutputSerializer;
import com.rackspacecloud.blueflood.types.Resolution;

import java.util.*;

public class PlotRequestParser {
    public static final Set<BasicRollupsOutputSerializer.MetricStat> DEFAULT_STATS = new HashSet<BasicRollupsOutputSerializer.MetricStat>();
    public static final Set<BasicRollupsOutputSerializer.MetricStat> DEFAULT_BASIC;
    public static final Set<BasicRollupsOutputSerializer.MetricStat> DEFAULT_COUNTER;
    public static final Set<BasicRollupsOutputSerializer.MetricStat> DEFAULT_GAUGE;
    public static final Set<BasicRollupsOutputSerializer.MetricStat> DEFAULT_SET;
    public static final Set<BasicRollupsOutputSerializer.MetricStat> DEFAULT_TIMER;
    
    static {
        // EnumSet is so crappy for making me do this instead of using an anonymous subclass.
        DEFAULT_BASIC = EnumSet.noneOf(BasicRollupsOutputSerializer.MetricStat.class);
        DEFAULT_COUNTER = EnumSet.noneOf(BasicRollupsOutputSerializer.MetricStat.class);
        DEFAULT_GAUGE = EnumSet.noneOf(BasicRollupsOutputSerializer.MetricStat.class);
        DEFAULT_SET = EnumSet.noneOf(BasicRollupsOutputSerializer.MetricStat.class);
        DEFAULT_TIMER = EnumSet.noneOf(BasicRollupsOutputSerializer.MetricStat.class);
        
        DEFAULT_BASIC.add(BasicRollupsOutputSerializer.MetricStat.AVERAGE);
        DEFAULT_BASIC.add(BasicRollupsOutputSerializer.MetricStat.NUM_POINTS);
        
        DEFAULT_COUNTER.add(BasicRollupsOutputSerializer.MetricStat.NUM_POINTS);
        DEFAULT_COUNTER.add(BasicRollupsOutputSerializer.MetricStat.SUM);

        DEFAULT_GAUGE.add(BasicRollupsOutputSerializer.MetricStat.NUM_POINTS);
        DEFAULT_GAUGE.add(BasicRollupsOutputSerializer.MetricStat.LATEST);
        
        DEFAULT_SET.add(BasicRollupsOutputSerializer.MetricStat.NUM_POINTS);
        
        DEFAULT_TIMER.add(BasicRollupsOutputSerializer.MetricStat.RATE);
        DEFAULT_TIMER.add(BasicRollupsOutputSerializer.MetricStat.NUM_POINTS);
        DEFAULT_TIMER.add(BasicRollupsOutputSerializer.MetricStat.AVERAGE);
        
        DEFAULT_STATS.add(BasicRollupsOutputSerializer.MetricStat.AVERAGE);
        DEFAULT_STATS.add(BasicRollupsOutputSerializer.MetricStat.NUM_POINTS);
    } 

    public static RollupsQueryParams parseParams(Map<String, List<String>> params) throws InvalidRequestException {
        if (params == null || params.isEmpty()) {
            throw new InvalidRequestException("No query parameters present.");
        }

        List<String> points = params.get("points");
        List<String> res = params.get("resolution");
        List<String> from = params.get("from");
        List<String> to = params.get("to");
        List<String> select = params.get("select");

        if (points == null && res == null) {
            throw new InvalidRequestException("Either 'points' or 'resolution' is required.");
        }

        if (points != null && points.size() != 1) {
            throw new InvalidRequestException("Invalid parameter: points=" + points);
        } else if (res != null && res.size() != 1) {
            throw new InvalidRequestException("Invalid parameter: resolution=" + res);
        } else if (from == null || from.size() != 1) {
            throw new InvalidRequestException("Invalid parameter: from=" + from);
        } else if (to == null || to.size() != 1) {
            throw new InvalidRequestException("Invalid parameter: to="+ to);
        }

        long fromTime = Long.parseLong(from.get(0));
        long toTime = Long.parseLong(to.get(0));

        if (toTime <= fromTime) {
            throw new InvalidRequestException("paramter 'to' must be greater than 'from'");
        }

        Set<BasicRollupsOutputSerializer.MetricStat> stats = getStatsToFilter(select);

        if (points != null) {
            try {
                return new RollupsQueryParams(fromTime, toTime, Integer.parseInt(points.get(0)), stats);
            } catch (NumberFormatException ex) {
                throw new InvalidRequestException("'points' param must be a valid integer");
            }
        } else {
            return new RollupsQueryParams(fromTime, toTime, Resolution.fromString(res.get(0)), stats);
        }
    }

    public static Set<BasicRollupsOutputSerializer.MetricStat> getStatsToFilter(List<String> select) {
        if (select == null || select.isEmpty()) {
            return DEFAULT_STATS;
        } else {
            Set<BasicRollupsOutputSerializer.MetricStat> filters = new HashSet<BasicRollupsOutputSerializer.MetricStat>();
            // handle case when someone does select=average,min instead of select=average&select=min
            for (String stat : select) {
                if (stat.contains(",")) {
                    List<String> nestedStats = Arrays.asList(stat.split(","));
                    filters.addAll(BasicRollupsOutputSerializer.MetricStat.fromStringList(nestedStats));
                } else {
                    BasicRollupsOutputSerializer.MetricStat possibleStat = BasicRollupsOutputSerializer.MetricStat.fromString(stat);
                    if (possibleStat != null)
                        filters.add(possibleStat);
                }
            }
            return filters;
        }
    }
}
