package com.rackspacecloud.blueflood.tools.jmx;

import com.yammer.metrics.util.JmxGauge;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

// JmxGauge does not handle booleans. This will typecast boolean to 0 or 1.
public class JmxBooleanGauge extends JmxGauge {
    public JmxBooleanGauge(String objectName, String attribute) throws MalformedObjectNameException {
        super(objectName, attribute);
    }

    public JmxBooleanGauge(ObjectName objectName, String attribute) {
        super(objectName, attribute);
    }

    @Override
    public Object value() {
        Object value = super.value();
        if (value.equals(true)) {
            return 1;
        }
        return 0;
    }
}
