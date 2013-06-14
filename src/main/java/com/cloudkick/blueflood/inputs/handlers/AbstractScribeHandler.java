package com.cloudkick.blueflood.inputs.handlers;

import org.apache.thrift.TException;
import scribe.thrift.LogEntry;
import scribe.thrift.ResultCode;
import scribe.thrift.scribe;

import java.util.List;

public abstract class AbstractScribeHandler implements scribe.Iface {
    public AbstractScribeHandler() {
    }
    
    public abstract ResultCode Log(final List<LogEntry> messages) throws TException;
}
