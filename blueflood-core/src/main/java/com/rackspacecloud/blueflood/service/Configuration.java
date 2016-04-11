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

package com.rackspacecloud.blueflood.service;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.net.URL;
import java.util.*;

public class Configuration {
    private static final Properties defaultProps = new Properties();
    private static Properties props;
    private static final Configuration INSTANCE = new Configuration();

    public static Configuration getInstance() {
        return INSTANCE;
    }

    private Configuration() {
        try {
            init();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void loadDefaults(ConfigDefaults[] configDefaults) {
        for (ConfigDefaults configDefault : configDefaults) {
            defaultProps.setProperty(configDefault.toString(), configDefault.getDefaultValue());
        }
    }

    public void init() throws IOException {
        props = new Properties(defaultProps);
        // load the configuration.
        String configStr = System.getProperty("blueflood.config");
        if (configStr != null) {
            URL configUrl = new URL(configStr);
            props.load(configUrl.openStream());
        }
    }

    /**
     * Convert the existing configuration values into a Map. Does not include defaults from the defaultProps object.
     *
     * @return an unmodifiable Map
     */
    public Map<Object,Object> getProperties() {
        return Collections.unmodifiableMap(props);
    }

    /**
     * Convert the existing configuration values into a Map, including those specified in defaultProps.
     *
     * @return an unmodifiable Map
     */
    public Map<Object, Object> getAllProperties() {

        Map<Object, Object> map = new HashMap<Object, Object>();
        for (Object key : defaultProps.keySet()) {
            map.put(key, defaultProps.getProperty(key.toString()));
        }
        for (Object key : props.keySet()) {
            map.put(key, props.getProperty(key.toString()));
        }

        return Collections.unmodifiableMap(map);
    }

    public String getStringProperty(Enum<? extends ConfigDefaults> name) {
        return getStringProperty(name.toString());
    }
    public String getStringProperty(String name) {
        if (System.getProperty(name) != null && !props.containsKey("original." + name)) {
            if (props.containsKey(name))
                props.put("original." + name, props.get(name));
            props.put(name, System.getProperty(name));
        }
        return props.getProperty(name);
    }

    public String getRawStringProperty(Enum<? extends ConfigDefaults> name) {
        return getRawStringProperty(name.toString());
    }
    public String getRawStringProperty(String name) {
        return props.getProperty(name);
    }

    public boolean containsKey(String key) {
        return props.containsKey(key);
    }

    public int getIntegerProperty(Enum<? extends ConfigDefaults> name) {
        return getIntegerProperty(name.toString());
    }
    public int getIntegerProperty(String name) {
        return intFromString(getStringProperty(name));
    }
    public static int intFromString(String value) {
        return Integer.parseInt(value);
    }

    public float getFloatProperty(Enum<? extends ConfigDefaults> name) {
        return getFloatProperty(name.toString());
    }
    public float getFloatProperty(String name) {
        return floatFromString(getStringProperty(name));
    }
    public static float floatFromString(String value) {
        return Float.parseFloat(value);
    }

    public long getLongProperty(Enum<? extends ConfigDefaults> name) {
        return getLongProperty(name.toString());
    }
    public long getLongProperty(String name) {
        return longFromString(getStringProperty(name));
    }
    public static long longFromString(String value) {
        return Long.parseLong(value);
    }

    public boolean getBooleanProperty(Enum<? extends ConfigDefaults> name) {
        return getBooleanProperty(name.toString());
    }
    public boolean getBooleanProperty(String name) {
        return booleanFromString(getStringProperty(name));
    }
    public static boolean booleanFromString(String value) {
        return "true".equalsIgnoreCase(value);
    }

    public List<String> getListProperty(Enum<? extends ConfigDefaults> name) {
        return getListProperty(name.toString());
    }
    public List<String> getListProperty(String name) {
        return stringListFromString(getStringProperty(name));
    }
    public static List<String> stringListFromString(String value) {
        List<String> list = Lists.newArrayList(value.split("\\s*,\\s*"));
        list.removeAll(Arrays.asList("", null));
        return list;
    }

    public void setProperty(String name, String val) {
      props.setProperty(name, val);
    }
    public void setProperty(Enum<? extends ConfigDefaults> name, String value) {
        setProperty(name.toString(), value);
    }

    public void clearProperty(String name) {
        props.remove(name);
    }
    public void clearProperty(Enum<? extends ConfigDefaults> name) {
        clearProperty(name.toString());
    }
}
