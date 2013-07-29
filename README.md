Blueflood - Metrics Ingestor and Rollup Coordinator
===================================================

# Introduction

# Getting Started

Building
--------
The Blueflood distribution gets created as part of `ant dist`.  If you're
impatient `ant jar-bf` will create the jar for you without assembling the
dependencies.

Running
-------
Several settings are important to set at runtime:

* __blueflood.config__: URL pointing to a valid Blueflood configuration file.
  An [annotated config file](https://github.com/racker/ele/blob/rollups2/java/conf/bf-dev.conf)
  is stored in github.
* __log4j.configuration__: Standard log4j configuration URL.
* __com.sun.management.jmxremote.authenticate__: normally false.
* __com.sun.management.jmxremote.ssl__: normally false
* __java.rmi.server.hostname__: specifies the interface that JMX will bind to. This
  value defaults to 0.0.0.0.
* __com.sun.management.jmxremote.port__: specifies the port JMX will listen on.
  This value defaults to 8080.

# Tuning

Blueflood can be tuned at runtime to adjust to changing metric patterns. This
is accomplished by exposing many settings through [JMX](http://www.oracle.com/technetwork/java/javase/tech/javamanagement-140525.html).

## RollupService
* __Active__: indicates that rollups can be scheduled. There is a poll method 
  that always looks for opportunities to schedule rollups, but none will be
  scheduled unless this flag is set.  After setting to __false__ it may take
  some time for pending rollups to drain.
* __InFlightRollupCount__: the number of rollups being executed right now.
* __KeepingServertime__: indicates whether or not the node is actively
  updating its clock (set to __false__ to control this externally).
* __PollerPeriod__: controls the frequency (in milliseconds) between when
  the RollupService looks for rollups that may be scheduled. A shorter period
  will result in the map of updates getting scanned more often (tightening
  rollup latency somewhat).
* __QueuedRollupCount__: the number of rollups waiting to be executed. This
  number is always much lower than the number of rollups that could be
  executed.
* __RollupConcurrency__: specifies the size of the threadpool that will
  perform rollups.  If QueuedRollupCount seems to be growing, increasing this
  value should allow more rollups to be executed.
* __ScheduledSlotCheckCount__: the number of slots that are scheduled and need
  to be rolled up. If your server is having a hard time keeping up this
  value will be high.  "High" is usually determined as some multiple of
  the number of shards this hode is responsible for.
* __SlotCheckConcurrency__: specifies the number of threads that process the
  scheduled checks.  Increasing this means that more rollups will be enqueued
  and will be available for execution.  If you set this value too high, you 
  will enqueue a lot of rollups and consume a lot of memory.

The goal of these tunables is to allow flexible configuration so that a rollup
slave will always have rollups to execute when they are available.

### Poll Timer
Monitors how long it takes for the poll() method to look for schedulable slots.
If poll times are unacceptably high, you should consider adding another
rollup slave.  See also RollupService.PollPeriod.

### Rejected Slot Checks
Monitors how often slot checks are rejected because no thread is available to 
service the request.  If this is regularly happening, you may wish to increase
RollupService.PollPeriod.

### Rollup Execution Timer
Monitors rollup execution throughput. Keeps track of how many rollups have been
executed, how long it takes and current rates of processing.  This is the best
guage of overall processing throughput.

### Rollup Wait Histogram
Monitors how long it takes from the time a rollup is enqueued until it is started.
When this value is high, it means that you may be enqueuing more rollups than the
number of threads specified and RollupService.RollupConcurrency can handle.

## ScribeHandler
Monitors:

* how long scribe Log() calls are taking.
* number of thrift messages received on the scribe interface.
* how much time is spent deserializing thrift messages.

## ShardStatePuller/ShardStatePusher

Allows you to set the frequency with which state updates are pushed/fetched
from the database.  Also monitors how long the calls take and if there were
any errors (such as timeouts).  The pusher/puller can also be turned off
through this interface.

## io.Instrumentation

This interface gives visibilty into Cassandra performance from Blueflood's
perspective.

Most exposed meters monitor how long it takes to perform some cassandra
operation.  Keep in mind that the histogram values are biased towards the
most recent 5 minutes of data.

### InstrumentatedConnectionPoolMonitor

Monitors the rates of cassandra read and write errors.  Spikes here 
may indicate systemic problems. Note that a high rate of 'Pool Exhausted'
exceptions with a multi-node Cassandra cluster is to be expected and not
problematic (except when seen alongside operation timeouts).
