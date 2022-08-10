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

import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Finds and loads modules from the classpath. A "module" in this sense is an optional component of Blueflood that's
 * specified in configuration. If no module of a certain type is configured, it's not an error. That functionality
 * simply isn't available.
 *
 * In configuration, modules are specified by giving their fully qualified class names as the values of certain
 * configuration items. Multiple modules may be specified for a single config item by separating their names with
 * commas. For this to work, the modules must be {@link QualifiedModule}s so that the loader can tell them apart. Like
 * in Spring Framework, qualifiers are used to distinguish between different modules of the same type. It's an error if
 * multiple modules have the same type and qualifier.
 *
 * It's only necessary to specify a qualifier if there's more than one module of that type. If only one module is
 * configured, you can ask for it without a qualifier, even if it has one. Unqualified modules are given a {@link
 * #DEFAULT_QUALIFIER default qualifier}, so you can also ask for unqualified modules with a qualifier, too.
 */
public class ModuleLoader {
    private static final Logger log = LoggerFactory.getLogger(ModuleLoader.class);

    // The default qualifier used for modules that don't implement QualifiedModule.
    public static final String DEFAULT_QUALIFIER = "default";

    // Cache of modules that have been loaded already. It's a two-level map where the outer key is the name of the
    // module and the inner key is the qualifier.
    private static final Map<String, Map<String, Object>> loadedModules = new HashMap<>();

    /**
     * Gets the instance of the given module. This is for when only one module of a type is configured. It will always
     * return the instance, regardless of the qualifier.
     *
     * @param moduleType the type of the module to get
     * @param moduleName the config item containing the name(s) of the modules(s)
     * @return the instance of the module, or null if none is configured
     */
    public static <T> T getInstance(Class<T> moduleType, CoreConfig moduleName) {
        return getInstance(moduleType, moduleName, ModuleLoader.DEFAULT_QUALIFIER);
    }

    /**
     * Gets the instance of the given module with the given qualifier. This method allows you to choose a module
     * instance by qualifier when there are multiple modules of the same type available.
     *
     * @param moduleType the type of the module to get
     * @param moduleName the config item containing the name(s) of the modules(s)
     * @param qualifier  the qualifier of the module to get
     * @return the instance of the module, or null if none is configured
     */
    public static <T> T getInstance(Class<T> moduleType, CoreConfig moduleName, String qualifier) {
        final T cachedModule = getCachedModule(moduleType, moduleName, qualifier);
        if (cachedModule != null) {
            return cachedModule;
        }
        cacheModules(moduleType, moduleName);
        return getCachedModule(moduleType, moduleName, qualifier);
    }

    /**
     * Loads all modules into the cache for the given config item. It's important to load all of them before trying to
     * find the right one because we have to know how to handle requests for a module if
     */
    private static <T> void cacheModules(Class<T> moduleType, CoreConfig moduleName) {
        for (String module : Configuration.getInstance().getListProperty(moduleName)) {
            log.info("Loading the module " + module);
            final T instance = loadInstance(moduleType, module);
            if (instance != null) {
                registerNewModule(moduleName, instance);
            }
        }
    }

    /**
     * Gets a module from the cache. If the request is for the default qualifier, and there's only one module for the
     * given name, return that module. This is to comply with the expected contract of getting a single module by type.
     */
    private static <T> T getCachedModule(Class<T> moduleType, CoreConfig moduleName, String qualifier) {
        Map<String, Object> qualifierCache = loadedModules.get(moduleName.toString());
        if (qualifierCache == null) {
            return null;
        }
        Object o = qualifierCache.get(qualifier);
        if (o == null && DEFAULT_QUALIFIER.equals(qualifier) && qualifierCache.size() == 1) {
            o = qualifierCache.values().iterator().next();
        }
        if (o == null) {
            return null;
        } else if (moduleType.isInstance(o)) {
            return moduleType.cast(o);
        } else {
            return null;
        }
    }

    /**
     * Registers a newly created module instance in the cache according to its qualifier, if any.
     */
    private static void registerNewModule(CoreConfig moduleName, Object moduleInstance) {
        Objects.requireNonNull(moduleInstance, "Module instance cannot be null");
        final String qualifier;
        if (moduleInstance instanceof QualifiedModule) {
            qualifier = ((QualifiedModule) moduleInstance).getQualifier();
        } else {
            qualifier = ModuleLoader.DEFAULT_QUALIFIER;
        }
        Map<String, Object> qualifierCache = loadedModules.computeIfAbsent(moduleName.toString(), k -> new HashMap<>());
        if (qualifierCache.containsKey(qualifier)) {
            throw new IllegalStateException("Module " + moduleName + " with qualifier " + qualifier +
                    " already exists");
        }
        qualifierCache.put(qualifier, moduleInstance);
    }

    /**
     * Loads a class from the classpath and converts it to the required type. If anything goes wrong, the error is
     * logged, and the method returns null.
     */
    private static <T> T loadInstance(Class<T> clazz, String className) {
        try {
            ClassLoader loader = clazz.getClassLoader();
            Class<?> genericClass = loader.loadClass(className);
            return clazz.cast(genericClass.newInstance());
        } catch (ClassCastException e) {
            log.error("Module " + className + " is not an instance of " + clazz.getName(), e);
        } catch (InstantiationException e) {
            log.error(String.format("Unable to create instance of %s class for %s", clazz.getName(), className), e);
        } catch (ClassNotFoundException e) {
            log.error("Unable to locate module: " + className, e);
        } catch (Throwable e) {
            log.error("Error starting module: " + className, e);
        }
        return null;
    }

    @VisibleForTesting
    public static void clearCache() {
        loadedModules.clear();
    }
}
