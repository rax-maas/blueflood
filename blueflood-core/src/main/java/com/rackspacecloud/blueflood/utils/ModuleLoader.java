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

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class ModuleLoader {
    private static final Logger log = LoggerFactory.getLogger(ModuleLoader.class);
    private static Map<String, Object> loadedModules = new HashMap<String, Object>();

    public static Object getInstance(Class c, CoreConfig moduleName) {

        Object moduleInstance = loadedModules.get(moduleName.name().toString());
        if (moduleInstance != null)
            return moduleInstance;
        List<String> modules = Configuration.getInstance().getListProperty(moduleName);
        if (modules.isEmpty())
            return null;
        if (!modules.isEmpty() && modules.size() != 1) {
            throw new RuntimeException("Cannot load service with more than one "+moduleName+" module");
        }

        String module = modules.get(0);
        log.info("Loading the module " + module);

        try {
            ClassLoader loader = c.getClassLoader();
            Class genericClass = loader.loadClass(module);
            moduleInstance = genericClass.newInstance();
            loadedModules.put(moduleName.name().toString(), moduleInstance);
            log.info("Registering the module " + module);
        }
        catch (InstantiationException e) {
            log.error(String.format("Unable to create instance of %s class for %s", c.getName(), module), e);
        } catch (IllegalAccessException e) {
            log.error("Error starting module: " + module, e);
        } catch (ClassNotFoundException e) {
            log.error("Unable to locate module: " + module, e);
        } catch (RuntimeException e) {
            log.error("Error starting module: " + module, e);
        } catch (Throwable e) {
            log.error("Error starting module: " + module, e);
        }

        return moduleInstance;
    }
}
