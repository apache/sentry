/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.provider.db.service.thrift;

import com.codahale.metrics.*;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.json.MetricsModule;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.service.thrift.SentryService;
import org.apache.sentry.service.thrift.SentryServiceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.io.File.createTempFile;
import static org.apache.sentry.provider.db.service.thrift.SentryMetricsServletContextListener.METRIC_REGISTRY;
import static org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A singleton class which holds metrics related utility functions as well as the list of metrics
 */
public final class SentryMetrics {
  public enum Reporting {
    JMX,
    CONSOLE,
    LOG,
    JSON,
  }

  private static final Logger LOGGER = LoggerFactory
          .getLogger(SentryMetrics.class);

  private static SentryMetrics sentryMetrics = null;
  private final AtomicBoolean reportingInitialized = new AtomicBoolean();
  private boolean gaugesAdded = false;
  private boolean sentryServiceGaugesAdded = false;

  final Timer createRoleTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "create-role"));
  final Timer dropRoleTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "drop-role"));
  final Timer grantRoleTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "grant-role"));
  final Timer revokeRoleTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "revoke-role"));
  final Timer grantTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "grant-privilege"));
  final Timer revokeTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "revoke-privilege"));

  final Timer dropPrivilegeTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "drop-privilege"));
  final Timer renamePrivilegeTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "rename-privilege"));

  final Timer listRolesByGroupTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "list-roles-by-group"));
  final Timer listPrivilegesByRoleTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "list-privileges-by-role"));
  final Timer listPrivilegesForProviderTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "list-privileges-for-provider"));
  final Timer listPrivilegesByAuthorizableTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "list-privileges-by-authorizable"));

  /**
   * Return a Timer with name.
   */
  public Timer getTimer(String name) {
    return METRIC_REGISTRY.timer(name);
  }

  /**
   * Return a Histogram with name.
   */
  public Histogram getHistogram(String name) {
    return METRIC_REGISTRY.histogram(name);
  }

  /**
   * Return a Counter with name.
   */
  public Counter getCounter(String name) {
    return METRIC_REGISTRY.counter(name);
  }

  private SentryMetrics() {
    registerMetricSet("gc", new GarbageCollectorMetricSet(), METRIC_REGISTRY);
    registerMetricSet("buffers",
            new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()),
        METRIC_REGISTRY);
    registerMetricSet("memory", new MemoryUsageGaugeSet(), METRIC_REGISTRY);
    registerMetricSet("threads", new ThreadStatesGaugeSet(), METRIC_REGISTRY);
  }

  public static synchronized SentryMetrics getInstance() {
    if (sentryMetrics == null) {
      sentryMetrics = new SentryMetrics();
    }
    return sentryMetrics;
  }

  void addSentryStoreGauges(SentryStore sentryStore) {
    if(!gaugesAdded) {
      addGauge(SentryStore.class, "role_count", sentryStore.getRoleCountGauge());
      addGauge(SentryStore.class, "privilege_count",
              sentryStore.getPrivilegeCountGauge());
      addGauge(SentryStore.class, "group_count", sentryStore.getGroupCountGauge());
      addGauge(SentryStore.class, "hms.waiters", sentryStore.getHMSWaitersCountGauge());
      addGauge(SentryStore.class, "hms.notification.id", sentryStore.getLastNotificationIdGauge());
      gaugesAdded = true;
    }
  }

  public void addSentryServiceGauges(SentryService sentryservice) {
    if(!sentryServiceGaugesAdded) {
      addGauge(SentryService.class, "is_active", sentryservice.getIsActiveGauge());
      addGauge(SentryService.class, "activated", sentryservice.getBecomeActiveCount());
      sentryServiceGaugesAdded = true;
    }
  }

  /**
   * Initialize reporters. Only initializes once.
   * <p>
   * Available reporters:
   * <ul>
   *     <li>console</li>
   *     <li>log</li>
   *     <li>jmx</li>
   * </ul>
   *
   * For console reporter configre it to report every
   * <em>SENTRY_REPORTER_INTERVAL_SEC</em> seconds.
   * <p>
   * Method is thread safe.
   */
  void initReporting(Configuration conf) {
    final String reporter = conf.get(ServerConfig.SENTRY_REPORTER);
    if ((reporter == null) || reporter.isEmpty() || reportingInitialized.getAndSet(true)) {
      // Nothing to do, just return
      return;
    }

    final int reportInterval =
            conf.getInt(ServerConfig.SENTRY_REPORTER_INTERVAL_SEC,
                    ServerConfig.SENTRY_REPORTER_INTERVAL_DEFAULT);

    switch(SentryMetrics.Reporting.valueOf(reporter.toUpperCase())) {
      case CONSOLE:
        LOGGER.info("Enabled console metrics reporter with {} seconds interval",
                reportInterval);
        final ConsoleReporter consoleReporter =
                ConsoleReporter.forRegistry(METRIC_REGISTRY)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
        consoleReporter.start(reportInterval, TimeUnit.SECONDS);
        break;
      case JMX:
        LOGGER.info("Enabled JMX metrics reporter");
        final JmxReporter jmxReporter = JmxReporter.forRegistry(METRIC_REGISTRY)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
        jmxReporter.start();
        break;
      case LOG:
        LOGGER.info("Enabled Log4J metrics reporter with {} seconds interval",
                reportInterval);
        final Slf4jReporter logReporter = Slf4jReporter.forRegistry(METRIC_REGISTRY)
                .outputTo(LOGGER)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        logReporter.start(reportInterval, TimeUnit.SECONDS);
        break;
      case JSON:
        LOGGER.info("Enabled JSON metrics reporter with {} seconds interval", reportInterval);
        JsonFileReporter jsonReporter = new JsonFileReporter(conf, reportInterval, TimeUnit.SECONDS);
        jsonReporter.start();
        break;
      default:
        LOGGER.warn("Invalid metrics reporter {}", reporter);
        break;
    }
  }

  private <T, V> void addGauge(Class<T> tClass, String gaugeName, Gauge<V> gauge) {
    METRIC_REGISTRY.register(
        name(tClass, gaugeName), gauge);
  }

  private void registerMetricSet(String prefix, MetricSet metricSet, MetricRegistry registry) {
    for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
      if (entry.getValue() instanceof MetricSet) {
        registerMetricSet(prefix + "." + entry.getKey(), (MetricSet) entry.getValue(), registry);
      } else {
        registry.register(prefix + "." + entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Custom reporter that writes metrics as a JSON file.
   * This class originated from Apache Hive JSON reporter.
   */
  private static class JsonFileReporter implements AutoCloseable, Runnable {
    /** File permissions: -rw-r--r-- */
    private static final short PERMISSIONS = 0644;
    private static final String JSON_REPORTER_THREAD_NAME = "json-reporter";

    private ScheduledExecutorService executor = null;
    private final ObjectMapper jsonMapper =
            new ObjectMapper().registerModule(new MetricsModule(TimeUnit.SECONDS,
                    TimeUnit.MILLISECONDS,
                    false));
    private final Configuration conf;
    /** Destination file name */
    private final String pathString;
    private final long interval;
    private final TimeUnit unit;

    JsonFileReporter(Configuration conf, long interval, TimeUnit unit) {
      this.conf = conf;
      pathString = conf.get(ServerConfig.SENTRY_JSON_REPORTER_FILE,
              ServerConfig.SENTRY_JSON_REPORTER_FILE_DEFAULT);
      this.interval = interval;
      this.unit = unit;
    }

    private void start() {
      executor = Executors.newScheduledThreadPool(1,
              new ThreadFactoryBuilder().setNameFormat(JSON_REPORTER_THREAD_NAME).build());
      executor.scheduleAtFixedRate(this, 0, interval, unit);
    }

    @Override
    public void run() {
      String json = null;
      try {
        json = jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(METRIC_REGISTRY);
      } catch (JsonProcessingException e) {
        LOGGER.error("Error converting metrics to JSON", e);
        return;
      }
      File tmpFile = null;
      try {
        tmpFile = createTempFile("sentry-json", null);
      } catch (IOException e) {
        LOGGER.error("failed to create temp file for JSON metrics", e);
      }

      assert tmpFile != null;
      try (LocalFileSystem fs = FileSystem.getLocal(conf);
           BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile))) {
        bw.write(json);
        Path tmpPath = new Path(tmpFile.getAbsolutePath());
        fs.setPermission(tmpPath, FsPermission.createImmutable(PERMISSIONS));
        Path path = new Path(pathString);
        fs.rename(tmpPath, path);
        fs.setPermission(path, FsPermission.createImmutable(PERMISSIONS));
      } catch (IOException e) {
        LOGGER.warn("Error writing JSON metrics", e);
      }
    }

    @Override
    public void close() {
      if (executor != null) {
        SentryServiceUtil.shutdownAndAwaitTermination(executor,
                JSON_REPORTER_THREAD_NAME, 1, TimeUnit.MINUTES, LOGGER);
        executor = null;
      }
    }
  }
}
