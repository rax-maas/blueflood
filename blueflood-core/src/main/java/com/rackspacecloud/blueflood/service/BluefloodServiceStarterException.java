package com.rackspacecloud.blueflood.service;

public class BluefloodServiceStarterException extends Exception {
    public BluefloodServiceStarterException(int status, String message) {
        super(message);

        this.status = status;
    }

    int status;
    public int getStatus() {
        return status;
    }
}
