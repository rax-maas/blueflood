package com.rackspacecloud.blueflood.dw.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Layout;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.stuartwarren.logit.logback.ZmqAppender;
import io.dropwizard.logging.AbstractAppenderFactory;
import io.dropwizard.validation.ValidationMethod;
import net.logstash.logback.encoder.LogstashEncoder;
import net.logstash.logback.layout.LogstashLayout;import java.lang.Override;import java.lang.String;

@JsonTypeName("rax-logstash-zmq")
public class ZmqLogstashAppenderFactory extends AbstractAppenderFactory {

    private static final String HOST = "host";
    private static final String INSTANCE_TYPE = "instanceType";
    private static final String INSTANCE = "instance";
    
    private String socketType;
    private String endpoints;
    private String bindConnect;
    private int linger;
    private int sendHWM;
    private String customFields;
    private boolean enabled;

    @Override
    public Appender<ILoggingEvent> build(LoggerContext context, String applicationName, Layout<ILoggingEvent> layout) {
        final ZmqAppender<ILoggingEvent> appender = new ZmqAppender<ILoggingEvent>() {
            @Override
            public void doAppend(ILoggingEvent eventObject) {
                super.doAppend(eventObject);
            }
        };
        appender.setSocketType(socketType);
        appender.setEndpoints(endpoints);
        appender.setBindConnect(bindConnect);
        appender.setLinger(linger);
        appender.setSendHWM(sendHWM);
        appender.setContext(context);
        
        if (layout == null) {
            LogstashLayout logstashLayout = new LogstashLayout();
            logstashLayout.setContext(context);
            logstashLayout.setIncludeCallerInfo(true);
            appender.setLayout(logstashLayout);
        } else {
            appender.setLayout(layout);
        }
        
        LogstashEncoder encoder = new LogstashEncoder();
        encoder.setIncludeCallerInfo(true);
        encoder.setContext(context);
        encoder.setCustomFields(customFields);
        appender.setEncoder(encoder);
        
        appender.stop();
        appender.start();
        
        return appender;
    }
    
    @JsonIgnore
    @ValidationMethod(message = "todo: write a valid validation")
    public boolean isValidArchiveConfiguration() {
        return true;
    }

    @JsonProperty
    public boolean isEnabled() { return enabled; }

    @JsonProperty
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    @JsonProperty
    public String getSocketType() { return socketType; }

    @JsonProperty
    public void setSocketType(String socketType) { this.socketType = socketType; }

    @JsonProperty
    public String getEndpoints() { return endpoints; }

    @JsonProperty
    public void setEndpoints(String endpoints) { this.endpoints = endpoints; }

    @JsonProperty
    public String getBindConnect() { return bindConnect; }

    @JsonProperty
    public void setBindConnect(String bindConnect) { this.bindConnect = bindConnect; }

    @JsonProperty
    public int getLinger() { return linger; }

    @JsonProperty
    public void setLinger(int linger) { this.linger = linger; }

    @JsonProperty
    public int getSendHWM() { return sendHWM; }

    @JsonProperty
    public void setSendHWM(int sendHWM) { this.sendHWM = sendHWM; }

    @JsonProperty
    public String getCustomFields() { return customFields; }

    @JsonProperty
    public void setCustomFields(String customFields) {
        
        // perform some replacements.
        for (String prop : new String[]{HOST, INSTANCE_TYPE, INSTANCE}) {
            if (System.getProperty(prop) != null) {
                customFields = customFields.replace("${" + prop + "}", System.getProperty(prop));
            }    
        }
        
        this.customFields = customFields; 
    }
}
