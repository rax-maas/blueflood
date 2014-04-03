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

package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.inputs.handlers.KafkaHandler;
import com.rackspacecloud.blueflood.io.AstyanaxMetricsWriter;
import com.rackspacecloud.blueflood.io.IMetricsWriter;

public class KafkaIngestionService implements IngestionService {
    private KafkaHandler ingestor;
    public void startService(ScheduleContext context, IMetricsWriter writer) {
        // TODO: figure out a way of having separate writers for separate ingestion services.
        ingestor = new KafkaHandler(context, new AstyanaxMetricsWriter());
    }
}
