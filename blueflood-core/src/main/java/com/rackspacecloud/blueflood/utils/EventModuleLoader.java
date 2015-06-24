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

import com.rackspacecloud.blueflood.io.GenericElasticSearchIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EventModuleLoader {
    private static final Logger log = LoggerFactory.getLogger(EventModuleLoader.class);
    private static GenericElasticSearchIO instance = null;

    public static GenericElasticSearchIO getInstance() {
        if (instance == null) {
            loadEventModule();
        }
        return instance;
    }

    public static synchronized void loadEventModule() {
        List<String> modules = Configuration.getInstance().getListProperty(CoreConfig.EVENTS_MODULES);

        if (modules.isEmpty() || instance != null)
            return;

        ClassLoader classLoader = GenericElasticSearchIO.class.getClassLoader();
        for (String module : modules) {
            log.info("Loading metric event module " + module);
            try {
                Class discoveryClass = classLoader.loadClass(module);
                instance = (GenericElasticSearchIO) discoveryClass.newInstance();
                log.info("Registering metric event module " + module);
            } catch (InstantiationException e) {
                log.error("Unable to create instance of metric event class for: " + module, e);
            } catch (IllegalAccessException e) {
                log.error("Error starting metric event module: " + module, e);
            } catch (ClassNotFoundException e) {
                log.error("Unable to locate metric event module: " + module, e);
            } catch (RuntimeException e) {
                log.error("Error starting metric event module: " + module, e);
            } catch (Throwable e) {
                log.error("Error starting metric event module: " + module, e);
            }
        }
    }
}
