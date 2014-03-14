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
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.annotate.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;

import java.io.IOException;

public class MetricsBatchSerializer implements Encoder<MetricsCollection>, Decoder<MetricsCollection> {

    public MetricsBatchSerializer(VerifiableProperties properties) {
        System.out.println(properties.toString());
    }

    public byte[] toBytes(MetricsCollection batch) {
        try {
            return getObjectMapper().writeValueAsBytes(batch);
        } catch (IOException e) {
            System.out.println("ERROR WITH SERIALIZATION");
            e.printStackTrace();
            return null;   // TODO
        }
    }

    public MetricsCollection fromBytes(byte[] bytes) {
        ObjectMapper mapper = getObjectMapper();
        try {
            //TODO handle error
            return mapper.readValue(bytes, MetricsCollection.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule metricModule = new SimpleModule("MetricsBatchSerializerModule", new Version(1, 0, 0, null));

        metricModule.setMixInAnnotation(IMetric.class, IMetricMixin.class);
        metricModule.setMixInAnnotation(Locator.class, LocatorMixin.class);
        metricModule.addDeserializer(Metric.class, new MetricDeserializer());

        mapper.setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.registerModule(metricModule);
        return mapper;
    }

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

