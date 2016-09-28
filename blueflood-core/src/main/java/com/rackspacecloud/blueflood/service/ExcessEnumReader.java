/*
 * Copyright 2015 Rackspace
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

import java.util.Collections;
import java.util.Set;

import com.codahale.metrics.Meter;

import com.rackspacecloud.blueflood.io.IOContainer;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExcessEnumReader implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ExcessEnumReader.class);
    private final Meter readMeter = Metrics.meter(ExcessEnumReader.class, 
                                                    "reads", "Cassandra Reads");
    private final Meter readErrMeter = Metrics.meter(ExcessEnumReader.class, 
                                                    "reads", "Cassandra Read Errors");
        

    private static final ExcessEnumReader INSTANCE = new ExcessEnumReader();
    public static ExcessEnumReader getInstance() {
        return INSTANCE;
    }

    private Set<Locator> excessEnumMetrics = Collections.emptySet();
    private static final Configuration config = Configuration.getInstance();

    public Boolean isInExcessEnumMetrics(Locator m){
        return excessEnumMetrics.contains(m);
    }
    final public void run() {

        int sleepMillis = config.getIntegerProperty(CoreConfig.EXCESS_ENUM_READER_SLEEP);
        // Loop and periodically read the table from Cassandra
        while (true)
        {
            try {
                Set<Locator> excess = IOContainer.fromConfig().getExcessEnumIO().getExcessEnumMetrics();
                excessEnumMetrics = excess;
                readMeter.mark();
                Thread.sleep(sleepMillis);
            } catch (Exception e) {
                log.error("ExcessEnumReader failed with exception " + e);
                readErrMeter.mark();
            }
        }
    }
}
