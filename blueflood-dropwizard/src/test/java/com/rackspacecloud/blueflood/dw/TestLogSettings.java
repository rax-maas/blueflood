package com.rackspacecloud.blueflood.dw;

import com.rackspacecloud.blueflood.dw.logging.LogSettings;
import junit.framework.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestLogSettings {
    
    private static LogSettings load(String path) throws IOException {
        File file = new File(path);
        Assert.assertTrue(String.format("Cannot load %s", file.getAbsolutePath()), file.exists());
        System.setProperty("log4j.configuration", "file://" + file.getAbsolutePath());
        return new LogSettings();
    }
    
    @Test
    public void testConsoleOnly() throws IOException {
        LogSettings settings = load("src/test/resources/console-only.properties");
        
        Assert.assertTrue(settings.hasConsoleAppender());
        Assert.assertFalse(settings.hasFileAppender());
    }
    
    @Test
    public void testFileOnly() throws IOException {
        LogSettings settings = load("src/test/resources/file-only.properties");
        
        Assert.assertFalse(settings.hasConsoleAppender());
        Assert.assertTrue(settings.hasFileAppender());
    }
    
    @Test
    public void testBoth() throws IOException {
        LogSettings settings = load("src/test/resources/both.properties");
                
        Assert.assertTrue(settings.hasConsoleAppender());
        Assert.assertTrue(settings.hasFileAppender());
        
        Assert.assertEquals("INFO", settings.getDefaultLogLevel());
        
        Assert.assertEquals("WARN", settings.getLoggers().get("httpclient.wire.header"));
        Assert.assertEquals("WARN", settings.getLoggers().get("httpclient.wire.content"));
        Assert.assertEquals("WARN", settings.getLoggers().get("org.apache.zookeeper.ClientCnxn"));
        Assert.assertEquals("ERROR", settings.getLoggers().get("org.apache.zookeeper.client.ZooKeeperSaslClient"));
        Assert.assertEquals("INFO", settings.getLoggers().get("org.apache.http.client.protocol"));
        Assert.assertEquals("INFO", settings.getLoggers().get("org.apache.http.wire"));
        Assert.assertEquals("INFO", settings.getLoggers().get("org.apache.http.impl"));
        Assert.assertEquals("INFO", settings.getLoggers().get("org.apache.http.headers"));
        
        Assert.assertEquals("%p %t %c - %m%n", settings.getFileFormatString());
        Assert.assertEquals("tests.log", settings.getFileName());
        
        Assert.assertEquals("%p %t %c", settings.getConsoleFormatString());
    }
}
