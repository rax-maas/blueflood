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

package com.rackspacecloud.blueflood.outputs.serializers;

import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Rollup;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface BasicRollupsOutputSerializer<T> {
    public T transformRollupData(MetricData metricData, Set<MetricStat> filterStats) throws SerializationException;

    public static enum MetricStat {
        AVERAGE("average") {
            @Override
            Object convertBasicRollupToObject(BasicRollup rollup) {
                return rollup.getAverage();
            }

            @Override
            Object convertRawSampleToObject(Object rawSample) {
                return rawSample;
            }
        },
        VARIANCE("variance") {
            @Override
            Object convertBasicRollupToObject(BasicRollup rollup) {
                return rollup.getVariance();
            }

            @Override
            Object convertRawSampleToObject(Object rawSample) {
                return 0;
            }
        },
        MIN("min") {
            @Override
            Object convertBasicRollupToObject(BasicRollup rollup) {
                return rollup.getMinValue();
            }

            @Override
            Object convertRawSampleToObject(Object rawSample) {
                return rawSample;
            }
        },
        MAX("max") {
            @Override
            Object convertBasicRollupToObject(BasicRollup rollup) {
                return rollup.getMaxValue();
            }

            @Override
            Object convertRawSampleToObject(Object rawSample) {
                return rawSample;
            }
        },
        NUM_POINTS("numPoints") {
            @Override
            Object convertBasicRollupToObject(BasicRollup rollup) {
                return rollup.getCount();
            }

            @Override
            Object convertRawSampleToObject(Object rawSample) {
                return 1;
            }
        };
        private MetricStat(String s) {
            this.stringRep = s;
        }
        private String stringRep;
        private static final Map<String, MetricStat> stringToEnum = new HashMap<String, MetricStat>();
        static {
            for (MetricStat ms : values()) {
                stringToEnum.put(ms.toString().toLowerCase(), ms);
            }
        }
        public static MetricStat fromString(String s) {
            return stringToEnum.get(s.toLowerCase());
        }
        public static Set<MetricStat> fromStringList(List<String> statList) {
            Set<MetricStat> set = new HashSet<MetricStat>();
            for (String stat : statList ) {
                MetricStat metricStat = fromString(stat);
                if (metricStat != null) {
                    set.add(fromString(stat));
                }
            }
            return set;
        }
        @Override
        public String toString() {
            return this.stringRep;
        }
        abstract Object convertBasicRollupToObject(BasicRollup rollup);
        abstract Object convertRawSampleToObject(Object rawSample);
    }
}

