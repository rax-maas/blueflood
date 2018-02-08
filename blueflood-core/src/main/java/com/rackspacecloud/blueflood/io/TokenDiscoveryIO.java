package com.rackspacecloud.blueflood.io;


import com.rackspacecloud.blueflood.types.Token;

import java.io.IOException;
import java.util.List;

public interface TokenDiscoveryIO extends MetricNameSearchIO {
    void insertDiscovery(Token token) throws IOException;
    void insertDiscovery(List<Token> tokens) throws IOException;
}
