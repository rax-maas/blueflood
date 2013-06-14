package com.cloudkick.blueflood.tools.jmx;

public class HostAndPort {
    private final String host;
    private final int port;
    
    private HostAndPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
    
    public static HostAndPort fromString(String s) {
        String[] parts = s.split(":", -1);
        return new HostAndPort(parts[0], Integer.parseInt(parts[1]));
    }
}
