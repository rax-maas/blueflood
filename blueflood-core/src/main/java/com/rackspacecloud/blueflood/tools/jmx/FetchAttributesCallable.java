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

package com.rackspacecloud.blueflood.tools.jmx;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.lang.reflect.Method;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.concurrent.Callable;

public class FetchAttributesCallable implements Callable<String[]> {
    public static Format DECIMAL_FORMAT = new DecimalFormat("0.00");
    
    private final HostAndPort hostInfo;
    private final ObjectName objectName;
    private final String[] attributes;
    
    public FetchAttributesCallable(HostAndPort hostInfo, ObjectName objectName, String[] attributes) {
        this.hostInfo = hostInfo;
        this.objectName = objectName;
        this.attributes = attributes;
    }
    
    
    public String[] call() throws Exception {
        JMXConnector connector = null;
        String[] values = new String[attributes.length];
        try {
            JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", hostInfo.getHost(), hostInfo.getPort()));
            connector = JMXConnectorFactory.connect(url);
            MBeanServerConnection connection = connector.getMBeanServerConnection();    
            Class mbeanClass = Class.forName(
                (String)connection.getMBeanInfo(objectName).getDescriptor().getFieldValue("interfaceClassName"));
            Object handle = JMX.newMBeanProxy(connection, objectName, mbeanClass, true);
            
            for (int i = 0; i < attributes.length; i++) {
                Method attrMethod = mbeanClass.getMethod("get" + attributes[i]);
                values[i] = asString(attrMethod.invoke(handle));
            }
            return values;
        } finally {
            if (connector != null)
                connector.close();
        }
    }
    
    private static String asString(Object obj) {
        if (obj == null)
            return "";
        else if (obj instanceof Double || obj instanceof Float)
            return DECIMAL_FORMAT.format(obj);
        else
            return obj.toString();
    }
}
