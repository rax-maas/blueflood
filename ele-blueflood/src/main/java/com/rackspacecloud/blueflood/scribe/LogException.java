package com.rackspacecloud.blueflood.scribe;

/** Indicates a problem logging to a connected scribe client. */
public class LogException extends Exception {
    public LogException(String msg, Exception parent) {
        super(msg, parent);
    }
}
