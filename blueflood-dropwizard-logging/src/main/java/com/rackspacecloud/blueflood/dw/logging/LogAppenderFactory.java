package com.rackspacecloud.blueflood.dw.logging;

import java.util.ArrayList;
import java.util.List;

public class LogAppenderFactory {
    private static final List<LogAppenderFactory> ALL = new ArrayList<LogAppenderFactory>() {{
        add(new LogAppenderFactory("AirbrakeAppenderFactory", "bf.logging.airbrake"));
        add(new LogAppenderFactory("ZmqLogstashAppenderFactory", "bf.logging.zmq-logstash"));
    }};
    
    public static Iterable<LogAppenderFactory> all() { return ALL; }
    
    private final String className;
    private final String vmFlag;
    
    private LogAppenderFactory(String className, String vmFlag) {
        this.className = className;
        this.vmFlag = vmFlag;
    }
    
    public String getAppenderClassName() { return className; }
    public String getVmFlag() { return vmFlag; }
}
