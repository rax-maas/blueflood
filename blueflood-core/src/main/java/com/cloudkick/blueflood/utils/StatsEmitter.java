package com.cloudkick.blueflood.utils;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.studyblue.metrics.reporting.StatsdReporter;
import com.yammer.metrics.core.VirtualMachineMetrics;

public class StatsEmitter extends StatsdReporter {

  private static final Logger log = LoggerFactory.getLogger(StatsEmitter.class);

  public static StatsEmitter create(String host, int port) {
    if (host == null || "NONE".equals(host)) {
      return null;
    }
    log.info("Starting statsd metrics reporter to {}:{}...", host, port);
    StatsEmitter stats = null;
    try {
      stats = new StatsEmitter(host, port);
    } catch (IOException ioe) {
      log.error("Error creating statsd reporter {}", ioe.getMessage());
    }
    return stats;
  }

  private StatsEmitter(String host, int port) throws IOException {
    super(host, port);

    this.start(5, TimeUnit.SECONDS);
  }

  // override this method to avoid immutable collection + jmx bug,
  // should not have to do this
  @Override
  protected void printVmMetrics(long epoch) {

    sendFloat("jvm.memory.totalInit", StatType.GAUGE, vm.totalInit());
    sendFloat("jvm.memory.totalUsed", StatType.GAUGE, vm.totalUsed());
    sendFloat("jvm.memory.totalMax", StatType.GAUGE, vm.totalMax());
    sendFloat("jvm.memory.totalCommitted", StatType.GAUGE, vm.totalCommitted());

    sendFloat("jvm.memory.heapInit", StatType.GAUGE, vm.heapInit());
    sendFloat("jvm.memory.heapUsed", StatType.GAUGE, vm.heapUsed());
    sendFloat("jvm.memory.heapMax", StatType.GAUGE, vm.heapMax());
    sendFloat("jvm.memory.heapCommitted", StatType.GAUGE, vm.heapCommitted());

    sendFloat("jvm.memory.heapUsage", StatType.GAUGE, vm.heapUsage());
    sendFloat("jvm.memory.nonHeapUsage", StatType.GAUGE, vm.nonHeapUsage());

    for (Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
      sendFloat("jvm.memory.memory_pool_usages." + sanitizeString(pool.getKey()), StatType.GAUGE, pool.getValue());
    }

    sendInt("jvm.daemon_thread_count", StatType.GAUGE, vm.daemonThreadCount());
    sendInt("jvm.thread_count", StatType.GAUGE, vm.threadCount());
    sendInt("jvm.uptime", StatType.GAUGE, vm.uptime());
    sendFloat("jvm.fd_usage", StatType.GAUGE, vm.fileDescriptorUsage());

    for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages().entrySet()) {
      sendFloat("jvm.thread-states." + entry.getKey().toString().toLowerCase(), StatType.GAUGE, entry.getValue());
    }

    for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
      final String name = "jvm.gc." + sanitizeString(entry.getKey());
      sendInt(name + ".time", StatType.GAUGE, entry.getValue().getTime(TimeUnit.MILLISECONDS));
      sendInt(name + ".runs", StatType.GAUGE, entry.getValue().getRuns());
    }
  }
}
