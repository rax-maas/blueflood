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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

/**
 * java/conf/bf-dev.con has an exhaustive description of each configuration option.
 *
 */
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

    public Map<Object,Object> getProperties() {
        return Collections.unmodifiableMap(props);
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

    public int getIntegerProperty(Enum<? extends ConfigDefaults> name) {
        return getIntegerProperty(name.toString());
    }
    public int getIntegerProperty(String name) {
        return Integer.parseInt(getStringProperty(name));
    }

    public float getFloatProperty(Enum<? extends ConfigDefaults> name) {
        return getFloatProperty(name.toString());
    }
    public float getFloatProperty(String name) {
        return Float.parseFloat(getStringProperty(name));
    }

    public long getLongProperty(Enum<? extends ConfigDefaults> name) {
        return getLongProperty(name.toString());
    }
    public long getLongProperty(String name) {
        return Long.parseLong(getStringProperty(name));
    }

    public boolean getBooleanProperty(Enum<? extends ConfigDefaults> name) {
        return getBooleanProperty(name.toString());
    }

    public boolean getBooleanProperty(String name) {
        return "true".equalsIgnoreCase(getStringProperty(name));
    }

    public List<String> getListProperty(Enum<? extends ConfigDefaults> name) {
        return getListProperty(name.toString());
    }
    public List<String> getListProperty(String name) {
        List<String> list = Lists.newArrayList(getStringProperty(name).split("\\s*,\\s*"));
        list.removeAll(Arrays.asList("", null));
        return list;
    }

    @VisibleForTesting
    public void setProperty(String name, String val) {
      props.setProperty(name, val);
    }
}
