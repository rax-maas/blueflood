/*
 * Copyright 2013-2015 Rackspace
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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.cache.LocatorCache;
import com.rackspacecloud.blueflood.concurrent.FunctionWithThreadPool;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DiscoveryWriter extends FunctionWithThreadPool<List<List<IMetric>>, Void> {

    private final List<DiscoveryIO> discoveryIOs = new ArrayList<DiscoveryIO>();
    private final Map<Class<? extends DiscoveryIO>, Meter> writeErrorMeters = new HashMap<Class<? extends DiscoveryIO>, Meter>();
    private static final Meter locatorsWritten =
            Metrics.meter(DiscoveryWriter.class, "Locators Written to Discovery");
    private static final Logger log = LoggerFactory.getLogger(DiscoveryWriter.class);
    private int maxNewLocatorsPerMinute;
    private int maxNewLocatorsPerMinutePerTenant;
    private final Cache<Object, Object> newLocatorsThrottle;
    private final LoadingCache<String, Cache<Locator, Locator>> newLocatorsThrottlePerTenant;
    private final boolean canIndex;

    public DiscoveryWriter(ThreadPoolExecutor threadPool) {
        super(threadPool);
        registerIOModules();
        this.canIndex = discoveryIOs.size() > 0;
        maxNewLocatorsPerMinute = Configuration.getInstance().getIntegerProperty(
                CoreConfig.DISCOVERY_MAX_NEW_LOCATORS_PER_MINUTE);
        maxNewLocatorsPerMinutePerTenant = Configuration.getInstance().getIntegerProperty(
                CoreConfig.DISCOVERY_MAX_NEW_LOCATORS_PER_MINUTE_PER_TENANT);
        newLocatorsThrottle = CacheBuilder.newBuilder()
                .concurrencyLevel(16).expireAfterWrite(1, TimeUnit.MINUTES).build();
        // This throttle is a cache of caches, one per tenant. It's convenient to have it automatically build the nested
        // cache on demand, rather than having to do it manually.
        newLocatorsThrottlePerTenant = CacheBuilder.newBuilder()
                .concurrencyLevel(16)
                .expireAfterAccess(1, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Cache<Locator, Locator>>() {
                           @Override
                           public Cache<Locator, Locator> load(String key) {
                               return CacheBuilder.newBuilder()
                                       .concurrencyLevel(16)
                                       .expireAfterWrite(1, TimeUnit.MINUTES)
                                       .build();
                           }
                       }
                );
    }

    public void setMaxNewLocatorsPerMinute(int maxNewLocatorsPerMinute) {
        this.maxNewLocatorsPerMinute = maxNewLocatorsPerMinute;
    }

    public void setMaxNewLocatorsPerMinutePerTenant(int maxNewLocatorsPerMinutePerTenant) {
        this.maxNewLocatorsPerMinutePerTenant = maxNewLocatorsPerMinutePerTenant;
    }

    public void registerIO(DiscoveryIO io) {
        discoveryIOs.add(io);
        writeErrorMeters.put(io.getClass(),
                Metrics.meter(io.getClass(), "DiscoveryWriter Write Errors")
                );
    }

    public void registerIOModules() {
        // TODO: These should really both just be set as DISCOVERY_MODULES. Then it's up to the user whether to use
        //       token discovery or not. This will do for now, since the query handlers require at most one module to be
        //       set in each of these. In the future, use the ModuleLoader with qualifiers to satisfy the query handler
        //       needs, and allow multiple DISCOVERY_MODULES to be set and used here. TOKEN_DISCOVERY_MODULES should be
        //       considered deprecated.
        DiscoveryIO discoveryIO = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);
        if (discoveryIO != null) {
            registerIO(discoveryIO);
        }
        if (Configuration.getInstance().getBooleanProperty(CoreConfig.ENABLE_TOKEN_SEARCH_IMPROVEMENTS)) {
            discoveryIO = ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.TOKEN_DISCOVERY_MODULES);
            if (discoveryIO != null) {
                registerIO(discoveryIO);
            }
        }
    }

    private List<IMetric> condense(List<List<IMetric>> input) throws ExecutionException {
        newLocatorsThrottle.cleanUp();
        Set<String> tenants = input.stream()
                .flatMap(Collection::stream)
                .map(IMetric::getLocator)
                .map(Locator::getTenantId)
                .collect(Collectors.toSet());
        for (String tenant : tenants) {
            newLocatorsThrottlePerTenant.get(tenant).cleanUp();
        }
        boolean isThrottlingGlobally = newLocatorsThrottle.size() >= maxNewLocatorsPerMinute;
        List<IMetric> willIndex = new ArrayList<IMetric>();
        for (List<IMetric> list : input) {
            // make mockito happy.
            if (list.size() == 0) {
                continue;
            }

            for (IMetric m : list) {
                boolean isAlreadySeen = LocatorCache.getInstance().isLocatorCurrentInDiscoveryLayer(m.getLocator());
                Cache<Locator, Locator> tenantThrottle = newLocatorsThrottlePerTenant.get(m.getLocator().getTenantId());
                boolean isTenantThrottled = tenantThrottle.size() >= maxNewLocatorsPerMinutePerTenant;
                if (!isAlreadySeen && !isThrottlingGlobally && !isTenantThrottled) {
                    willIndex.add(m);
                }
            }
        }
        return willIndex;
    }

    public ListenableFuture<Boolean> processMetrics(final List<List<IMetric>> input) {
        int remainingCapacityOfTheBlockingQueue = this.remainingCapacityOfTheQueue();
        log.debug("Remaining capacity of DiscoveryWriter blocking queue: [{}]", remainingCapacityOfTheBlockingQueue);

        if(remainingCapacityOfTheBlockingQueue == 0){
            log.warn("Remaining capacity of DiscoveryWriter blocking queue: [{}]", remainingCapacityOfTheBlockingQueue);
        }

        // process en masse.
        return getThreadPool().submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                boolean success = true;
                // filter out the metrics that are current.
                final List<IMetric> willIndex = condense(input);

                if (willIndex.size() > 0) {
                    locatorsWritten.mark(willIndex.size());
                    for (DiscoveryIO io : discoveryIOs) {
                        try {
                            io.insertDiscovery(willIndex);
                        } catch (Exception ex) {
                            getLogger().error(ex.getMessage(), ex);
                            writeErrorMeters.get(io.getClass()).mark();
                            success = false;
                        }
                    }
                }

                if(success) {
                    //when all metrics have been written successfully, mark them as current.
                    for(IMetric indexedMetric: willIndex) {
                        Locator locator = indexedMetric.getLocator();
                        LocatorCache.getInstance().setLocatorCurrentInDiscoveryLayer(locator);
                        newLocatorsThrottle.put(locator, locator);
                        newLocatorsThrottlePerTenant.get(locator.getTenantId()).put(locator, locator);
                    }
                }

                return success;
            }
        });
    }

    @Override
    public Void apply(List<List<IMetric>> input) {
        if (canIndex) {
            processMetrics(input);
        }
	return null;
    }
}
