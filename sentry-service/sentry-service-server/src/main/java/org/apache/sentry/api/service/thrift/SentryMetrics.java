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
package org.apache.sentry.api.service.thrift;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.json.MetricsModule;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.persistent.SentryStoreInterface;
import org.apache.sentry.service.thrift.SentryService;
import org.apache.sentry.api.common.SentryServiceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.codahale.metrics.MetricRegistry.name;
import static org.apache.sentry.api.service.thrift.SentryMetricsServletContextListener.METRIC_REGISTRY;
import static org.apache.sentry.service.common.ServiceConstants.ServerConfig;

/**
 * A singleton class which holds metrics related utility functions as well as the list of metrics.
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
  final Timer listPrivilegesByUserTimer = METRIC_REGISTRY.timer(
    name(SentryPolicyStoreProcessor.class, "list-privileges-by-user"));
  final Timer listPrivilegesForUserTimer = METRIC_REGISTRY.timer(
          name(SentryPolicyStoreProcessor.class, "list-sentry-privileges-by-user-and-itsgroups"));
  final Timer listPrivilegesForProviderTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "list-privileges-for-provider"));
  final Timer listPrivilegesByAuthorizableTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "list-privileges-by-authorizable"));
  final Timer listRolesPrivilegesTimer = METRIC_REGISTRY.timer(
      name(SentryPolicyStoreProcessor.class, "list-roles-privileges"));
  final Timer listUsersPrivilegesTimer = METRIC_REGISTRY.timer(
    name(SentryPolicyStoreProcessor.class, "list-users-privileges"));
  final Timer notificationProcessTimer = METRIC_REGISTRY.timer(
          name(SentryPolicyStoreProcessor.class, "process-hsm-notification"));

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

  /**
   * Get singleton instance.
   */
  public static synchronized SentryMetrics getInstance() {
    if (sentryMetrics == null) {
      sentryMetrics = new SentryMetrics();
    }
    return sentryMetrics;
  }

  void addSentryStoreGauges(SentryStoreInterface sentryStore) {
    if (!gaugesAdded) {
      addGauge(SentryStore.class, "role_count", sentryStore.getRoleCountGauge());
      addGauge(SentryStore.class, "privilege_count",
              sentryStore.getPrivilegeCountGauge());
      addGauge(SentryStore.class, "gm_privilege_count",
          sentryStore.getGenericModelPrivilegeCountGauge());
      addGauge(SentryStore.class, "group_count", sentryStore.getGroupCountGauge());
      addGauge(SentryStore.class, "hms.waiters", sentryStore.getHMSWaitersCountGauge());
      addGauge(SentryStore.class, "hms.notification.id",
          sentryStore.getLastNotificationIdGauge());
      addGauge(SentryStore.class, "hms.snapshot.paths.id",
          sentryStore.getLastPathsSnapshotIdGauge());
      addGauge(SentryStore.class, "hms.perm.change.id",
          sentryStore.getPermChangeIdGauge());
      addGauge(SentryStore.class, "hms.path.change.id",
          sentryStore.getPathChangeIdGauge());
      addGauge(SentryStore.class, "hms.authz_objects_count",
          sentryStore.getAuthzObjectsCountGauge());
      addGauge(SentryStore.class, "hms.authz_paths_count",
          sentryStore.getAuthzPathsCountGauge());
      gaugesAdded = true;
    }
  }

  /**
   * Add gauges for the SentryService class.
   * @param sentryservice
   */
  public void addSentryServiceGauges(SentryService sentryservice) {
    if (!sentryServiceGaugesAdded) {
      addGauge(SentryService.class, "is_active", sentryservice.getIsActiveGauge());
      addGauge(SentryService.class, "activated", sentryservice.getBecomeActiveCount());
      sentryServiceGaugesAdded = true;
    }
  }

  /**
   * Initialize reporters. Only initializes once.<p>
   *
   * Available reporters:
   * <ul>
   *     <li>console</li>
   *     <li>log</li>
   *     <li>jmx</li>
   * </ul>
   *
   * <p><For console reporter configre it to report every
   * <em>SENTRY_REPORTER_INTERVAL_SEC</em> seconds.
   *
   * <p>Method is thread safe.
   */
  @SuppressWarnings("squid:S2095")
  void initReporting(Configuration conf) {
    final String reporter = conf.get(ServerConfig.SENTRY_REPORTER);
    if ((reporter == null) || reporter.isEmpty() || reportingInitialized.getAndSet(true)) {
      // Nothing to do, just return
      return;
    }

    final int reportInterval =
            conf.getInt(ServerConfig.SENTRY_REPORTER_INTERVAL_SEC,
                    ServerConfig.SENTRY_REPORTER_INTERVAL_DEFAULT);

    // Get list of configured reporters
    Set<String> reporters = new HashSet<>();
    for (String r: reporter.split(",")) {
      reporters.add(r.trim().toUpperCase());
    }

    // In case there are no reporters, configure JSON reporter
    if (reporters.isEmpty()) {
      reporters.add(Reporting.JSON.toString());
    }

    // Configure all reporters
    for (String r: reporters) {
      switch (SentryMetrics.Reporting.valueOf(r)) {
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
          JsonFileReporter jsonReporter = new JsonFileReporter(conf,
                  reportInterval, TimeUnit.SECONDS);
          jsonReporter.start();
          break;
        default:
          LOGGER.warn("Invalid metrics reporter {}", reporter);
          break;
      }
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
    //
    // Implementation notes.
    //
    // 1. Since only local file systems are supported, there is no need to use Hadoop
    //    version of Path class.
    // 2. java.nio package provides modern implementation of file and directory operations
    //    which is better then the traditional java.io, so we are using it here.
    //    In particular, it supports atomic creation of temporary files with specified
    //    permissions in the specified directory. This also avoids various attacks possible
    //    when temp file name is generated first, followed by file creation.
    //    See http://www.oracle.com/technetwork/articles/javase/nio-139333.html for
    //    the description of NIO API and
    //    http://docs.oracle.com/javase/tutorial/essential/io/legacy.html for the
    //    description of interoperability between legacy IO api vs NIO API.
    // 3. To avoid race conditions with readers of the metrics file, the implementation
    //    dumps metrics to a temporary file in the same directory as the actual metrics
    //    file and then renames it to the destination. Since both are located on the same
    //    filesystem, this rename is likely to be atomic (as long as the underlying OS
    //    support atomic renames.
    //

    // Permissions for the metrics file
    private static final FileAttribute<Set<PosixFilePermission>> FILE_ATTRS =
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
    private static final String JSON_REPORTER_THREAD_NAME = "json-reporter";

    private ScheduledExecutorService executor = null;
    private final ObjectMapper jsonMapper =
            new ObjectMapper().registerModule(new MetricsModule(TimeUnit.SECONDS,
                    TimeUnit.MILLISECONDS,
                    false));
    private final Configuration conf;
    /** Destination file name. */
    // Location of JSON file
    private final Path path;
    // tmpdir is the dirname(path)
    private final Path tmpDir;
    private final long interval;
    private final TimeUnit unit;

    JsonFileReporter(Configuration conf, long interval, TimeUnit unit) {
      this.conf = conf;
      String pathString = conf.get(ServerConfig.SENTRY_JSON_REPORTER_FILE,
              ServerConfig.SENTRY_JSON_REPORTER_FILE_DEFAULT);
      path = Paths.get(pathString).toAbsolutePath();
      LOGGER.info("Reporting metrics to {}", path);
      // We want to use tmpDir i the same directory as the destination file to support atomic
      // move of temp file to the destination metrics file
      tmpDir = path.getParent();
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
      Path tmpFile = null;
      try {
        String json = null;
        try {
          json = jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(METRIC_REGISTRY);
        } catch (JsonProcessingException e) {
          LOGGER.error("Error converting metrics to JSON", e);
          return;
        }
        // Metrics are first dumped to a temp file which is then renamed to the destination
        try {
          tmpFile = Files.createTempFile(tmpDir, "smetrics", "json", FILE_ATTRS);
        } catch (IOException e) {
          LOGGER.error("failed to create temp file for JSON metrics", e);
          return;
        } catch (SecurityException e) {
          // This shouldn't ever happen
          LOGGER.error("failed to create temp file for JSON metrics: no permissions", e);
          return;
        } catch (UnsupportedOperationException e) {
          // This shouldn't ever happen
          LOGGER.error("failed to create temp file for JSON metrics: operartion not supported", e);
          return;
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile.toFile()))) {
          bw.write(json);
        }

        // Move temp file to the destination file
        try {
          Files.move(tmpFile, path, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
          LOGGER.error("Failed to move temp metrics file to {}: {}", path, e.getMessage());
        }
      } catch (Throwable t) {
        // catch all errors (throwable and execptions to prevent subsequent tasks from being suppressed)
        LOGGER.error("Error executing scheduled task ", t);
      } finally {
        // If something happened and we were not able to rename the temp file, attempt to remove it
        if (tmpFile != null && tmpFile.toFile().exists()) {
          // Attempt to delete temp file, if this fails, not much can be done about it.
          try {
            Files.delete(tmpFile);
          } catch (Exception e) {
            LOGGER.error("failed to delete yemporary metrics file {}", tmpFile, e);
          }
        }
      }
    }

    @Override
    public void close() {
      if (executor != null) {
        SentryServiceUtil.shutdownAndAwaitTermination(executor,
                JSON_REPORTER_THREAD_NAME, 1, TimeUnit.MINUTES, LOGGER);
        executor = null;
      }
      try {
        Files.delete(path);
      } catch (IOException e) {
        LOGGER.error("Unable to delete {}", path, e);
      }
    }
  }
}
