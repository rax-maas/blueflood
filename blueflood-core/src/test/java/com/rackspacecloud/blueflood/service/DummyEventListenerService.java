package com.rackspacecloud.blueflood.service;

public class DummyEventListenerService implements EventListenerService {

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
