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

package com.rackspacecloud.blueflood.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.apache.commons.codec.digest.DigestUtils;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class Util {
    public static final String DEFAULT_DIMENSION = "default";
    public static final Format DECIMAL_FORMAT = new DecimalFormat("0.00");
    private static final Cache<String, Integer> shardCache = CacheBuilder.newBuilder().expireAfterAccess(10,
            TimeUnit.MINUTES).concurrencyLevel(30).build();

    public static Integer getShard(String s) {
        Integer shard = shardCache.getIfPresent(s);
        if (shard == null) {
            shard = computeShard(s);
            shardCache.put(s, shard);
        }
        return shard;
    }

    public static int computeShard(String s) {
        return (int)Long.parseLong(DigestUtils.md5Hex(s).substring(30), 16) % Constants.NUMBER_OF_SHARDS;
    }
    
    public static Collection<Integer> parseShards(String s) {
        ArrayList<Integer> list = new ArrayList<Integer>();
        if ("ALL".equalsIgnoreCase(s)) {
            for (int i = 0; i < Constants.NUMBER_OF_SHARDS; i++)
                list.add(i);
        } else if ("NONE".equalsIgnoreCase(s)) {
            return list;
        } else {
            for (String part : s.split(",", -1)) {
                int i = Integer.parseInt(part.trim());
                if (i >= Constants.NUMBER_OF_SHARDS || i < 0)
                    throw new NumberFormatException("Invalid shard identifier: " + part.trim());
                list.add(i);
            }
        }
        return list;
    }

    public static String getDimensionFromKey(String persistedMetric) {
       return persistedMetric.split("\\.", -1)[0];
    }

    public static String getMetricFromKey(String persistedMetric) {
        return persistedMetric.split("\\.", 2)[1];
    }
    
    public static double safeDiv(double numerator, double denominator) {
        if (denominator == 0)
            return 0d;
        else
            return numerator / denominator;
    }

    public static String ElasticIOPath = "com.rackspacecloud.blueflood.io.ElasticIO".intern();

    public static String UNKNOWN = "unknown".intern();

    public static boolean shouldUseESForUnits() {
        return Configuration.getInstance().getBooleanProperty(CoreConfig.USE_ES_FOR_UNITS) &&
                Configuration.getInstance().getListProperty(CoreConfig.DISCOVERY_MODULES).contains(ElasticIOPath);
    }
}
