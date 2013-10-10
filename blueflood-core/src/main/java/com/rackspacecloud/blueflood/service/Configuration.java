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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

/**
 * java/conf/bf-dev.con has an exhaustive description of each configuration option.
 */
public class Configuration {
    private static final String defaultPropFileName = "blueflood.properties";
    private static final Properties defaultProps = new Properties();
    private static Properties props;


    public static void init() throws IOException {
        //InputStream is = Configuration.class.getResourceAsStream(defaultPropFileName);
        InputStream is = Configuration.class.getResourceAsStream("/" + defaultPropFileName);
        //InputStream is = Configuration.class.getClassLoader().getResourceAsStream("resources/" + defaultPropFileName);
        defaultProps.load(is);
        is.close();
        props = new Properties(defaultProps);
        // load the configuration.
        String configStr = System.getProperty("blueflood.config");
        if (configStr != null) {
            URL configUrl = new URL(configStr);
            props.load(configUrl.openStream());
        }
    }

    public static Map<Object,Object> getProperties() {
        return Collections.unmodifiableMap(props);
    }

    public static String getStringProperty(String name) {
        if (System.getProperty(name) != null && !props.containsKey("original." + name)) {
            if (props.containsKey(name))
                props.put("original." + name, props.get(name));
            props.put(name, System.getProperty(name));
        }
        return props.getProperty(name);
    }

    public static int getIntegerProperty(String name) {
        return Integer.parseInt(getStringProperty(name));
    }

    public static float getFloatProperty(String name) {
        return Float.parseFloat(getStringProperty(name));
    }

    public static long getLongProperty(String name) {
        return Long.parseLong(getStringProperty(name));
    }

    public static boolean getBooleanProperty(String name) {
        return getStringProperty(name).equalsIgnoreCase("true");
    }

    public static List<String> getListProperty(String name) {
        List<String> list = new ArrayList<String>(Arrays.asList(getStringProperty(name).split("\\s*,\\s*")));
        list.removeAll(Arrays.asList("", null));
        return list;
    }
}
