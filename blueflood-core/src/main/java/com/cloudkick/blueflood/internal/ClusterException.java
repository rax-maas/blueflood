package com.cloudkick.blueflood.internal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** puts a bunch of exceptions together. */
public class ClusterException extends IOException {
    private Map<String, Throwable> exceptions = new HashMap<String, Throwable>();
    
    public void append(String host, Throwable ex) {
        exceptions.put(host, ex);
    }
    
    public Throwable getException(String host) { return exceptions.get(host); }
    
    public Iterable<Throwable> getExceptions() {
        return new Iterable<Throwable>() {
            public Iterator<Throwable> iterator() {
                return exceptions.values().iterator();
            }
        };
    }
    
    public int size() { return exceptions.size(); }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Throwable> entry : exceptions.entrySet())
            sb = sb.append(entry.getValue().getMessage()).append(", ");
        return sb.toString();
    }
}
