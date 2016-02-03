package com.rackspacecloud.blueflood.service;

import java.util.ArrayList;
import java.util.List;

public class DummyEventListenerService implements EventListenerService {

    static List<DummyEventListenerService> instances = new ArrayList<DummyEventListenerService>();
    public static List<DummyEventListenerService> getInstances() {
        return instances;
    }
    public static DummyEventListenerService getMostRecentInstance() {
        return instances.get(instances.size() - 1);
    }

    public DummyEventListenerService() {
        instances.add(this);
    }

    boolean startServiceCalled = false;
    public boolean getStartServiceCalled() {
        return startServiceCalled;
    }

    @Override
    public void startService() {
        if (startServiceCalled) {
            throw new UnsupportedOperationException("startService was called more than once");
        }
        startServiceCalled = true;
    }

    boolean stopServiceCalled = false;
    public boolean getStopServiceCalled() {
        return stopServiceCalled;
    }

    @Override
    public void stopService() {
        if (!startServiceCalled) {
            throw new UnsupportedOperationException("stopService was called before startService");
        }
        if (stopServiceCalled) {
            throw new UnsupportedOperationException("stopService was called more than once");
        }
        stopServiceCalled = true;
    }
}
