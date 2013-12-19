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

package com.rackspacecloud.blueflood.statsd;

import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.statsd.containers.Conversions;
import com.rackspacecloud.blueflood.statsd.containers.StatCollection;
import com.rackspacecloud.blueflood.statsd.containers.Stat;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Stats sent to graphite from statsd come in this format:
 * {label} {value} {timtestamp}
 */
public class StatParser extends AsyncFunctionWithThreadPool<Collection<CharSequence>, StatCollection> {
    private static Logger log = LoggerFactory.getLogger(StatParser.class);
    private static Meter invalidStatsMeter = Metrics.meter(StatParser.class, "Stat Parser Invalid Stats");
    private static Meter statParseErrors = Metrics.meter(StatParser.class, "Stat Parse Errors");
    
    private final StatsdOptions parseOptions;
    
    public StatParser(ThreadPoolExecutor executor, StatsdOptions parseOptions) {
        super(executor);
        this.parseOptions = parseOptions;
    }

    @Override
    public ListenableFuture<StatCollection> apply(final Collection<CharSequence> input) throws Exception {
        return getThreadPool().submit(new Callable<StatCollection>() {
            @Override
            public StatCollection call() throws Exception {
                StatCollection stats = new StatCollection();
                for (CharSequence seq : input) {
                    try {
                        Stat stat = Conversions.asStat(seq, parseOptions);
                        if (stat.isValid()) {
                            stats.add(stat);
                        } else {
                            log.debug("Invalid stat line: {}", seq.toString());
                            invalidStatsMeter.mark();
                        }
                    } catch (Throwable nastyParseBug) {
                        log.error(nastyParseBug.getMessage(), nastyParseBug);
                        statParseErrors.mark();
                    }
                }
                return stats;
            }
        });
    }
}
