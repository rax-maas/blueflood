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

package com.rackspacecloud.blueflood.inputs.processors;

import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.concurrent.NoOpFuture;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.utils.Metrics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscoveryWriter extends AsyncFunctionWithThreadPool<List<List<IMetric>>, List<List<IMetric>>> {

    private final List<DiscoveryIO> discoveryIOs = new ArrayList<DiscoveryIO>();
    private final Map<Class<? extends DiscoveryIO>, Meter> writeErrorMeters = new HashMap<Class<? extends DiscoveryIO>, Meter>();
    private static final Logger log = LoggerFactory.getLogger(DiscoveryWriter.class);
    private final boolean canIndex;

    public DiscoveryWriter(ThreadPoolExecutor threadPool) {
        super(threadPool);
        registerIOModules();
        this.canIndex = discoveryIOs.size() > 0;
    }

    public void registerIO(DiscoveryIO io) {
        discoveryIOs.add(io);
        writeErrorMeters.put(io.getClass(),
                Metrics.meter(io.getClass(), "DiscoveryWriter Write Errors")
                );
    }

    public void registerIOModules() {
        List<String> modules = Configuration.getInstance().getListProperty(CoreConfig.DISCOVERY_MODULES);

        ClassLoader classLoader = DiscoveryIO.class.getClassLoader();
        for (String module : modules) {
            log.info("Loading metric discovery module " + module);
            try {
                Class discoveryClass = classLoader.loadClass(module);
                DiscoveryIO discoveryIOModule = (DiscoveryIO) discoveryClass.newInstance();
                log.info("Registering metric discovery module " + module);
                registerIO(discoveryIOModule);
            } catch (InstantiationException e) {
                log.error("Unable to create instance of metric discovery class for: " + module, e);
            } catch (IllegalAccessException e) {
                log.error("Error starting metric discovery module: " + module, e);
            } catch (ClassNotFoundException e) {
                log.error("Unable to locate metric discovery module: " + module, e);
            } catch (RuntimeException e) {
                log.error("Error starting metric discovery module: " + module, e);
            } catch (Throwable e) {
                log.error("Error starting metric discovery module: " + module, e);
            }
        }
    }

    private static List<IMetric> condense(List<List<IMetric>> input) {
        List<IMetric> willIndex = new ArrayList<IMetric>();
        for (List<IMetric> list : input) {
            // make mockito happy.
            if (list.size() == 0) {
                continue;
            }

            for (IMetric m : list) {
                if (!AstyanaxWriter.isLocatorCurrent(m.getLocator())) {
                    willIndex.add(m);
                }
            }
        }
        return willIndex;
    }
    
    
    public ListenableFuture<Boolean> processMetrics(List<List<IMetric>> input) {
        // filter out the metrics that are current.
        final List<IMetric> willIndex = DiscoveryWriter.condense(input);

        // process en masse.
        return getThreadPool().submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                boolean success = true;
                for (DiscoveryIO io : discoveryIOs) {
                    try {
                        io.insertDiscovery(willIndex);
                    } catch (Exception ex) {
                        getLogger().error(ex.getMessage(), ex);
                        writeErrorMeters.get(io.getClass()).mark();
                        success = false;
                    }
                }
                return success;
            }
        });
    }

    public ListenableFuture<List<List<IMetric>>> apply(List<List<IMetric>> input) {
        if (canIndex) {
            processMetrics(input);
        }
        
        // we don't need all metrics to finish being inserted into the discovery backend
        // before moving onto the next step in the processing chain.
        return new NoOpFuture<List<List<IMetric>>>(input);
    }
}
