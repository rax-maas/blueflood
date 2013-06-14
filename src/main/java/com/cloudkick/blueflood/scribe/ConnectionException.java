package com.cloudkick.blueflood.scribe;

/** Indicates a problem connecting to a scribe instance */
public class ConnectionException extends Exception {
    public ConnectionException(String msg) {
        super(msg);
    }
}
