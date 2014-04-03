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

package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.outputs.handlers.KafkaProducer;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class KafkaMetricsWriter implements IMetricsWriter {
    private static final Logger log = LoggerFactory.getLogger(KafkaMetricsWriter.class);

    private final KafkaProducer kafka;

    public KafkaMetricsWriter() {
        try {
            kafka = KafkaProducer.getInstance();
        } catch (IOException e) {
            throw new RuntimeException("Unable to get Kafka Producer to write to!", e);
        }
    }

    @Override
    public void insertFullMetrics(Collection<Metric> metrics) throws IOException {
        log.debug("inserting full res metrics into kafka");
        kafka.pushFullResBatch(new ArrayList<IMetric>(metrics));
    }

    @Override
    public void insertPreaggreatedMetrics(Collection<IMetric> metrics) throws IOException {
        log.debug("inserting preagg metrics into kafka");
        kafka.pushPreaggregatedBatch(metrics);
    }
}
