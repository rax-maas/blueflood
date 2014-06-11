package com.rackspacecloud.blueflood.exceptions;

/** throw this when data is invalid (usually on ingest). */
public class InvalidDataException extends RuntimeException {
    public InvalidDataException(String message) {
        super(message);
    }
}
