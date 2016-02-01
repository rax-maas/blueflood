package com.rackspacecloud.blueflood.service;

import java.util.ArrayList;
import java.util.List;

public class DummyQueryService implements QueryService {

    static List<DummyQueryService> instances = new ArrayList<DummyQueryService>();
    public static List<DummyQueryService> getInstances() {
        return instances;
    }
    public static DummyQueryService getMostRecentInstance() {
        return instances.get(instances.size() - 1);
    }

    public DummyQueryService() {
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
}
