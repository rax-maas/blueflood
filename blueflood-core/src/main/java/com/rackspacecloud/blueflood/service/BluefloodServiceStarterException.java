package com.rackspacecloud.blueflood.service;

public class BluefloodServiceStarterException extends RuntimeException {
    public BluefloodServiceStarterException(int status, String message) {
        super(message);

        this.status = status;
    }
    public BluefloodServiceStarterException(int status, String message, Throwable cause) {
        super(message, cause);

        this.status = status;
    }

    int status;
    public int getStatus() {
        return status;
    }
}
