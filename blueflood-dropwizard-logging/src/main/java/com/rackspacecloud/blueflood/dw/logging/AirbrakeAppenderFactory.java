package com.rackspacecloud.blueflood.dw.logging;

import airbrake.AirbrakeNotice;
import airbrake.AirbrakeNotifier;
import airbrake.Backtrace;
import airbrake.BacktraceLine;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.logging.AbstractAppenderFactory;
import io.dropwizard.validation.ValidationMethod;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonTypeName("rax-airbrake")
public class AirbrakeAppenderFactory extends AbstractAppenderFactory {
    
    private final AirbrakeNotifier airbrakeNotifier = new AirbrakeNotifier();
    
    private String apiKey;
    private String env;
    private boolean enabled;
    
    @Override
    public Appender<ILoggingEvent> build(LoggerContext context, String applicationName, Layout<ILoggingEvent> layout) {
        final Appender<ILoggingEvent> appender = new AppenderBase<ILoggingEvent>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (!enabled) {
                    return;
                }
                
                if (event.getThrowableProxy() != null) {
                    airbrakeNotifier.notify(noticeFor(event.getThrowableProxy()));
                }  
            }
        };
        appender.stop();
        appender.start();
        return appender;
    }
    
    @JsonIgnore
    @ValidationMethod(message = "todo: write a valid validation")
    public boolean isValidArchiveConfiguration() {
        return apiKey != null && env != null;
    }
    
    private AirbrakeNotice noticeFor(IThrowableProxy throwable) {
        final Map<String, Object> request = new HashMap<String, Object>();
        final Map<String, Object> session = new HashMap<String, Object>();
        final Map<String, Object> environment = new HashMap<String, Object>();
        final List<String> environmentFilters = new ArrayList<String>();
        
        // todo: figure out the right values for projectRoot, hasRequest, and component.
        return new AirbrakeNotice(
                apiKey,
                "projectRoot",
                env,
                throwable.getMessage(),
                throwable.getClassName(),
                backTraceFor(throwable),
                request,
                session,
                environment,
                environmentFilters,
                false, // hasRequest
                "url_is_unused", // url,
                "component" // component.
        );
    }
    
    private Backtrace backTraceFor(IThrowableProxy throwable) {
        List<String> lines = new ArrayList<String>();
        for (StackTraceElementProxy traceLine : throwable.getStackTraceElementProxyArray()) {
            BacktraceLine btLine = new BacktraceLine(
                    traceLine.getStackTraceElement().getClassName(),
                    traceLine.getStackTraceElement().getFileName(),
                    traceLine.getStackTraceElement().getLineNumber(),
                    traceLine.getStackTraceElement().getMethodName()
            );
            lines.add(btLine.toString());
        }
        return new Backtrace(lines);
    }

    @JsonProperty
    public String getApiKey() { return apiKey; }

    @JsonProperty
    public void setApiKey(String apiKey) { this.apiKey = apiKey; }

    @JsonProperty
    public String getEnv() { return env; }

    @JsonProperty
    public void setEnv(String env) { this.env = env; }

    @JsonProperty
    public boolean isEnabled() { return enabled; }

    @JsonProperty
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    
    
}
