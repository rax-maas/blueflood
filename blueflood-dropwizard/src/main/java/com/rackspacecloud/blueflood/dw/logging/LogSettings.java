package com.rackspacecloud.blueflood.dw.logging;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LogSettings {
    private static final String DEFAULT_FORMAT = "%p %t %c - %m%n";
    private static final String DEFAULT_FILE_NAME = "blueflood.log";
    
    private final Properties props = new Properties();
    private final String consoleAppenderKey;
    private final String fileAppenderKey;
    
    public LogSettings() throws IOException {
        String console = null;
        String file = null;
        String log4jResource = System.getProperty("log4j.configuration");
        
        if (log4jResource != null) {
            URL url = new URL(log4jResource);
            props.load(url.openStream());
            
            for (Map.Entry<Object, Object> entry : props.entrySet()) {
                if (entry.getValue().toString().equals("org.apache.log4j.RollingFileAppender")) {
                    file = entry.getKey().toString();
                } else if (entry.getValue().toString().equals("org.apache.log4j.ConsoleAppender")) {
                    console = entry.getKey().toString();
                }
            }
        }
        
        consoleAppenderKey = console;
        fileAppenderKey = file;
    }
    
    public boolean hasConsoleAppender() {
        return consoleAppenderKey != null;
    }
    
    public boolean hasFileAppender() {
        return fileAppenderKey != null;
    }
    
    public String getConsoleFormatString() {
        String pattern = props.getProperty(consoleAppenderKey + ".layout.ConversionPattern");
        return pattern == null ? DEFAULT_FORMAT : pattern;
    }
    
    public String getFileFormatString() {
        String pattern = props.getProperty(fileAppenderKey + ".layout.ConversionPattern");
        return pattern == null ? DEFAULT_FORMAT : pattern;
    }
    
    public String getFileName() {
        String pattern = props.getProperty(fileAppenderKey + ".File");
        return pattern == null ? DEFAULT_FILE_NAME : pattern;
    }
    
    public String getDefaultLogLevel() {
        String value = props.getProperty("log4j.rootLogger");
        if (value == null) {
            return "INFO";
        } else {
            try {
                return value.split(",", -1)[0].trim();
            } catch (Throwable th) {
                return "INFO";
            }
        }
    }
    
    // gets special loggers for classes or packages.
    public Map<String, String> getLoggers() {
        Map<String, String> loggers = new HashMap<String, String>();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            
            if (key.startsWith("log4j.logger.")) {
                loggers.put(stripPrefix(key, "log4j.logger."), value);
            } else if (key.startsWith("log4j.category.")) {
                loggers.put(stripPrefix(key, "log4j.category."), value);
            }
        }
        return loggers;
    }
    
    private static String stripPrefix(String s, String prefix) {
        if (!s.startsWith(prefix)) {
            throw new RuntimeException("Doesn't start with prefix");
        }
        return s.substring(prefix.length());
    }
    
    private static class LogEntry implements Map.Entry<String, String> {
        private final String key;
        private final String value;
        
        public LogEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }
        
        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String setValue(String value) {
            throw new RuntimeException("Not implemented");
        }
    }
    
    public static String toDropwizardFormat(LogSettings settings) {
        StringBuilder sb = new StringBuilder();
        Map<String, String> loggers = settings.getLoggers();
        sb = sb.append("logging:\n");
        sb = sb.append(String.format("  level: %s\n", settings.getDefaultLogLevel()));
        
        if (loggers.size() > 0) {
            sb = sb.append("  loggers:\n");
            for (Map.Entry<String, String> entry : loggers.entrySet()) {
                sb = sb.append(String.format("    %s: %s\n", entry.getKey(), entry.getValue()));
            }
        }
        
        if (settings.hasConsoleAppender() || settings.hasFileAppender()) {
            sb = sb.append("  appenders:\n");
        }
        if (settings.hasConsoleAppender()) {
            sb = sb.append("    - type: console\n");
            //sb = sb.append("      threshold: ?\n");
            sb = sb.append("      timeZone: UTC\n");
            sb = sb.append("      target: stdout\n");
            sb = sb.append(String.format("      logFormat: \"%s\"\n", settings.getConsoleFormatString()));
        }
        
        if (settings.hasFileAppender()) {
            sb = sb.append("    - type: file\n");
            //sb = sb.append("      threshold: ?\n");
            sb = sb.append("      timeZone: UTC\n");
            sb = sb.append(String.format("      currentLogFilename: %s\n", settings.getFileName()));
            sb = sb.append(String.format("      logFormat: \"%s\"\n", settings.getFileFormatString()));
            sb = sb.append("      archive: false\n"); // punt
        }
        
        return sb.toString();
        
    }
}
