package com.rackspacecloud.blueflood.exceptions;

import java.io.IOException;

public class SerializationException extends IOException {
    public SerializationException(String message) {
        super(message);
    }
}
