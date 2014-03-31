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

package com.rackspacecloud.blueflood.inputs.processors;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.inputs.handlers.KafkaConsumer;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.KafkaConsumerConfig;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.Metrics;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerWorker implements Runnable {
    private final KafkaStream<String, IMetric> stream;
    private final AsyncChain<MetricsCollection, List<Boolean>> processorChain;
    private static final Integer BATCH_SIZE = Configuration.getInstance().getIntegerProperty(KafkaConsumerConfig.KAFKA_CONSUMER_MAX_BATCH_SIZE);
    private static final Timer loopTimer = Metrics.timer(KafkaConsumerWorker.class, "Kafka Collect and Persist Time");
    private static final Timer innerTimer = Metrics.timer(KafkaConsumerWorker.class, "Kafka Collect Time");
    private static final Timer persistTimer = Metrics.timer(KafkaConsumerWorker.class, "Kafka Persist Time");
    private static final Meter meter = Metrics.meter(KafkaConsumerWorker.class, "Kafka Messages Read");
    private static final Meter errorMeter = Metrics.meter(KafkaConsumerWorker.class, "Kafka Error Persisting/Retry");
    // Collect Timeouts just mean that you end up writing a batch that is smaller than MAX_BATCH_SIZE, after waiting for KAFKA_CONSUMER_TIMEOUT_MS;
    private static final Meter partialBatchMeter = Metrics.meter(KafkaConsumerWorker.class, "Kafka Collect Timeout");
    private final Counter bufferedMetrics;

    public KafkaConsumerWorker(Counter bufferedMetrics, AsyncChain<MetricsCollection, List<Boolean>> metrics) {
        this.bufferedMetrics = bufferedMetrics;
        this.stream = KafkaConsumer.getInstance().getStream();
        this.processorChain = metrics;
    }

    @Override
    public void run() {
        ConsumerIterator<String, IMetric> iterator = stream.iterator();
        MetricsCollection m = new MetricsCollection();

        while (true) {
            Timer.Context loopCtx = loopTimer.time();
            Timer.Context collectTimingCtx = null;
            Timer.Context persistCtx = null;
            try {
                collectTimingCtx = innerTimer.time();
                // iterator.hasNext() blocks until there is another.
                try {
                    while(m.size() < BATCH_SIZE && iterator.hasNext()) {
                        MessageAndMetadata<String, IMetric> messageAndMetadata = iterator.next();
                        meter.mark();
                        m.add(messageAndMetadata.message());
                    }
                } catch (ConsumerTimeoutException e) {
                    partialBatchMeter.mark();
                    iterator.logger().debug("Timed out waiting on more messages to fill a batch. Writing a smaller batch.", e);
                }

                collectTimingCtx.stop();
                collectTimingCtx = null;

                persistCtx = persistTimer.time();
                ListenableFuture<List<Boolean>> future = processorChain.apply(m);
                List<Boolean> results = future.get(1l, TimeUnit.MINUTES);
                persistCtx.stop();
                boolean persisted = true;
                for (Boolean result : results) {
                    if (!result) {
                        persisted = false;
                    }
                }

                if (persisted) {
                    iterator.logger().info("Persisted a metricsCollection. Committing offsets");
                    KafkaConsumer.getInstance().commitOffsets();
                    m = new MetricsCollection();
                } else {
                    errorMeter.mark();
                    iterator.logger().warn("Unable to persist MetricsCollection. Will try again.");
                    // going to retry.
                }
            } catch (InterruptedException e) {
                iterator.logger().warn("Got interrupted exception while consuming.", e);
                break;
            } catch (Exception e) {
                iterator.logger().error("Exception in consuming.", e);
            } finally {
                if (collectTimingCtx != null) {
                    collectTimingCtx.stop();
                }
                if (persistCtx != null) {
                    persistCtx.stop();
                }
                loopCtx.stop();
            }
        }
    }
}
