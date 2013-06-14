package com.cloudkick.blueflood.inputs.handlers;

import org.apache.thrift.TException;
import scribe.thrift.LogEntry;
import scribe.thrift.ResultCode;
import scribe.thrift.scribe;

import java.util.List;

public interface ScribeHandlerIface {
    public scribe.Iface getScribe();
    public ResultCode Log(List<LogEntry> messages) throws TException;
}
