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

package com.rackspacecloud.blueflood.io.serializers;

import com.rackspacecloud.blueflood.types.*;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.annotate.*;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;

public class IMetricSerializer {
    public ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule metricModule = new SimpleModule("IMetricSerializerModule", new Version(1, 0, 0, null));

        metricModule.setMixInAnnotation(IMetric.class, IMetricMixin.class);
        metricModule.setMixInAnnotation(Locator.class, LocatorMixin.class);
        metricModule.setMixInAnnotation(PreaggregatedMetric.class, PreaggMetricMixin.class);
        metricModule.setMixInAnnotation(Rollup.class, RollupMixin.class);
        metricModule.setMixInAnnotation(BasicRollup.class, BasicRollupMixin.class);
        metricModule.setMixInAnnotation(AbstractRollupStat.class, AbstractRollupStatMixin.class);
        metricModule.setMixInAnnotation(GaugeRollup.class, GaugeRollupMixin.class);
        metricModule.setMixInAnnotation(TimerRollup.class, TimerRollupMixin.class);
        metricModule.addSerializer(AbstractRollupStat.class, new AbstractRollupStatSerializer());
        metricModule.setMixInAnnotation(TimerRollup.Percentile.class, PercentileMixin.class);
        metricModule.addDeserializer(Metric.class, new MetricDeserializer());
        metricModule.addDeserializer(GaugeRollup.class, new GaugeRollupDeserializer());

        mapper.setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        mapper.registerModule(metricModule);
        return mapper;
    }


    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = CounterRollup.class, name = "counter"),
            @JsonSubTypes.Type(value = GaugeRollup.class, name = "gauge"),
            @JsonSubTypes.Type(value = SetRollup.class, name = "set"),
            @JsonSubTypes.Type(value = TimerRollup.class, name = "timer"),
            @JsonSubTypes.Type(value = BasicRollup.class, name = "basic")
    })
    @JsonIgnoreProperties({ "rollupType" })
    abstract class RollupMixin {}

    abstract class PercentileMixin {
        @JsonValue
        abstract Number getMean();
    }

    abstract class BasicRollupMixin {
        @JsonIgnore
        abstract void setAverage(Average avg);

        @JsonIgnore
        abstract void setVariance(Variance var);

        @JsonIgnore
        abstract void setMin(MinValue min);

        @JsonIgnore
        abstract void setMax(MaxValue max);

        @JsonProperty("mean")
        abstract Average getAverage();

        @JsonProperty("var")
        abstract Variance getVariance();

        @JsonProperty("min")
        abstract MinValue getMinValue();

        @JsonProperty("max")
        abstract MaxValue getMaxValue();
    }

    @JsonIgnoreProperties({ "minValue", "maxValue", "rollupType" })
    abstract class TimerRollupMixin { }

    @JsonIgnoreProperties({ "latestValue", "rollupType" })
    abstract class GaugeRollupMixin { }

    abstract class AbstractRollupStatMixin {
        @JsonIgnore
        abstract byte getStatType();

        @JsonIgnore
        abstract boolean isFloatingPoint();
    }

    @JsonIgnoreProperties({ "ttl", "value", "type", "rollupType" })
    abstract class PreaggMetricMixin { }

    @JsonIgnoreProperties({ "stringRep" })
    abstract class LocatorMixin { }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "class")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = Metric.class, name = "raw"),
            @JsonSubTypes.Type(value = PreaggregatedMetric.class, name = "preaggregated") })
    @JsonIgnoreProperties({ "rollupType" })
    abstract class IMetricMixin {
        @JsonIgnore
        abstract boolean isNumeric();

        @JsonIgnore
        abstract boolean isString();

        @JsonIgnore
        abstract boolean isBoolean();

        @JsonUnwrapped
        abstract Locator getLocator();

        @JsonUnwrapped
        abstract DataType getDataType();
    }
}

