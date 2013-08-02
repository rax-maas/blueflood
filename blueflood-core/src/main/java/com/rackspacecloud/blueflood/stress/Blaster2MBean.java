package com.rackspacecloud.blueflood.stress;

import java.io.IOException;

public interface Blaster2MBean {
    
    public void setWriteSlaves(String s);
    public String getWriteSlaves();
    
    public void setNumChecks(int i);
    public int getNumChecks();
    
    public void setMetricsPerCheck(int i);
    public int getMetricsPerCheck();
    
    public void setNumThreads(int i);
    public int getNumThreads();
    public int getUnpushedMetricCount();
    
    public void addChecks(int num);
    public void go();
    public void stop();
    public boolean isBlasting();
    
    public int getTotalChecks();
    public void dumpStrings();
    
    public long getTotalEnqueued();
    public long getTotalLogged();
    
    public void addOverTime(int checkPerSecond, int numSeconds);
    
    public void save(String path) throws IOException;
    public void load(String path) throws IOException;
}
