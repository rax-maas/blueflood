package com.rackspacecloud.blueflood.service;

import java.util.ArrayList;
import java.util.List;

public class DummyIngestionService implements IngestionService {

    static List<DummyIngestionService> instances = new ArrayList<DummyIngestionService>();
    public static List<DummyIngestionService> getInstances() {
        return instances;
    }
    public static DummyIngestionService getMostRecentInstance() {
        return instances.get(instances.size() - 1);
    }

    public DummyIngestionService() {
        instances.add(this);
    }

    boolean startServiceCalled = false;
    public boolean getStartServiceCalled() {
        return startServiceCalled;
    }
    ScheduleContext context;
    public ScheduleContext getContext() {
        return context;
    }

    @Override
    public void startService(ScheduleContext context) {
        if (startServiceCalled) {
            throw new UnsupportedOperationException("startService was called more than once");
        }
        startServiceCalled = true;
        this.context = context;
    }

    boolean shutdownServiceCalled = false;
    public boolean getShutdownServiceCalled() {
        return shutdownServiceCalled;
    }

    @Override
    public void shutdownService() {
        if (!startServiceCalled) {
            throw new UnsupportedOperationException("shutdownService was called before startService");
        }
        if (shutdownServiceCalled) {
            throw new UnsupportedOperationException("shutdownService was called more than once");
        }
        shutdownServiceCalled = true;
    }
}
