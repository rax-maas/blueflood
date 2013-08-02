package com.cloudkick.blueflood.exceptions;

public class CacheException extends Exception {
    public CacheException(Throwable cause) {
        super(cause);
    }
    
    public CacheException(String wha) {
        super(wha);
    }
}
