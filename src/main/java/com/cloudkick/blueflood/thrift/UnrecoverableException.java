package com.cloudkick.blueflood.thrift;

public class UnrecoverableException extends RuntimeException {
    public UnrecoverableException(String msg) {
        super(msg);
    }
    
    public UnrecoverableException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
