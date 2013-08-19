/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.internal;

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
