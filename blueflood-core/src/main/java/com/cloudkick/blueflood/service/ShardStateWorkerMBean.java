package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.utils.TimeValue;

import java.util.concurrent.TimeUnit;

public interface ShardStateWorkerMBean {
    public void setActive(boolean b);
    public boolean getActive();
    
    public void force();
    
    public void setPeriod(long period);
    public long getPeriod();
}
