package com.rackspacecloud.blueflood.service;

public class DummyQueryService implements QueryService {

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
