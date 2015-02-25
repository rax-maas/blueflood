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

package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class QueryDiscoveryModuleLoader {
    private static final Logger log = LoggerFactory.getLogger(QueryDiscoveryModuleLoader.class);
    private static DiscoveryIO discoveryInstance;


    public static synchronized void loadDiscoveryModule() {
        if (discoveryInstance != null) {
            log.warn("Discovery module %s already loaded", discoveryInstance.getClass());
            return;
        }
        List<String> modules = Configuration.getInstance().getListProperty(CoreConfig.DISCOVERY_MODULES);

        if (modules.isEmpty())
            return;

        if (!modules.isEmpty() && modules.size() != 1) {
            throw new RuntimeException("Cannot load query service with more than one discovery module");
        }

        ClassLoader classLoader = DiscoveryIO.class.getClassLoader();
        String module = modules.get(0);
        log.info("Loading metric discovery module " + module);
        try {
            Class discoveryClass = classLoader.loadClass(module);
            discoveryInstance = (DiscoveryIO) discoveryClass.newInstance();
            log.info("Registering metric discovery module " + module);
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

    public static DiscoveryIO getDiscoveryInstance() {
        if (discoveryInstance == null) {
            // Try loading the discovery module instance
            loadDiscoveryModule();
        }
        return discoveryInstance;
    }
}
