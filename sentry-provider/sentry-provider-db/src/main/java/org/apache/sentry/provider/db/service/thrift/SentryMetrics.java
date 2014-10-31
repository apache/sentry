/**
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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import org.apache.sentry.provider.db.service.persistent.SentryStore;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A singleton class which holds metrics related utility functions as well as the list of metrics
 */
public class SentryMetrics {
  private static SentryMetrics sentryMetrics = null;
  private boolean reportingInitialized = false;
  private boolean gaugesAdded = false;

  public final Timer createRoleTimer = SentryMetricsServletContextListener.METRIC_REGISTRY.timer(
      MetricRegistry.name(SentryPolicyStoreProcessor.class, "create-role"));
  public final Timer dropRoleTimer = SentryMetricsServletContextListener.METRIC_REGISTRY.timer(
      MetricRegistry.name(SentryPolicyStoreProcessor.class, "drop-role"));
  public final Timer grantRoleTimer = SentryMetricsServletContextListener.METRIC_REGISTRY.timer(
      MetricRegistry.name(SentryPolicyStoreProcessor.class, "grant-role"));
  public final Timer revokeRoleTimer = SentryMetricsServletContextListener.METRIC_REGISTRY.timer(
      MetricRegistry.name(SentryPolicyStoreProcessor.class, "revoke-role"));
  public final Timer grantTimer = SentryMetricsServletContextListener.METRIC_REGISTRY.timer(
      MetricRegistry.name(SentryPolicyStoreProcessor.class, "grant-privilege"));
  public final Timer revokeTimer = SentryMetricsServletContextListener.METRIC_REGISTRY.timer(
      MetricRegistry.name(SentryPolicyStoreProcessor.class, "revoke-privilege"));

  public final Timer dropPrivilegeTimer = SentryMetricsServletContextListener.METRIC_REGISTRY.timer(
      MetricRegistry.name(SentryPolicyStoreProcessor.class, "drop-privilege"));
  public final Timer renamePrivilegeTimer = SentryMetricsServletContextListener.METRIC_REGISTRY.timer(
      MetricRegistry.name(SentryPolicyStoreProcessor.class, "rename-privilege"));

  public final Timer listRolesByGroupTimer = SentryMetricsServletContextListener.METRIC_REGISTRY.timer(
      MetricRegistry.name(SentryPolicyStoreProcessor.class, "list-roles-by-group"));
  public final Timer listPrivilegesByRoleTimer = SentryMetricsServletContextListener.METRIC_REGISTRY.timer(
      MetricRegistry.name(SentryPolicyStoreProcessor.class, "list-privileges-by-role"));
  public final Timer listPrivilegesForProviderTimer = SentryMetricsServletContextListener.METRIC_REGISTRY.timer(
      MetricRegistry.name(SentryPolicyStoreProcessor.class, "list-privileges-for-provider"));
  public final Timer listPrivilegesByAuthorizableTimer = SentryMetricsServletContextListener.METRIC_REGISTRY.timer(
      MetricRegistry.name(SentryPolicyStoreProcessor.class, "list-privileges-by-authorizable"));

  private SentryMetrics() {
    registerMetricSet("gc", new GarbageCollectorMetricSet(), SentryMetricsServletContextListener.METRIC_REGISTRY);
    registerMetricSet("buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()),
        SentryMetricsServletContextListener.METRIC_REGISTRY);
    registerMetricSet("memory", new MemoryUsageGaugeSet(), SentryMetricsServletContextListener.METRIC_REGISTRY);
    registerMetricSet("threads", new ThreadStatesGaugeSet(), SentryMetricsServletContextListener.METRIC_REGISTRY);
  }

  public static synchronized SentryMetrics getInstance() {
    if (sentryMetrics == null) {
      sentryMetrics = new SentryMetrics();
    }
    return sentryMetrics;
  }

  public void addSentryStoreGauges(SentryStore sentryStore) {
    if(!gaugesAdded) {
      addGauge(SentryStore.class, "role_count", sentryStore.getRoleCountGauge());
      addGauge(SentryStore.class, "privilege_count", sentryStore.getPrivilegeCountGauge());
      addGauge(SentryStore.class, "group_count", sentryStore.getGroupCountGauge());
      gaugesAdded = true;
    }
  }


  /* Should be only called once to initialize the reporters
   */
  public synchronized void initReporting(Reporting reporting) {
    if(!reportingInitialized) {
      switch(reporting) {
        case CONSOLE:
          final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(SentryMetricsServletContextListener.METRIC_REGISTRY)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .build();
          consoleReporter.start(1, TimeUnit.SECONDS);
          break;
        case JMX:
          final JmxReporter jmxReporter = JmxReporter.forRegistry(SentryMetricsServletContextListener.METRIC_REGISTRY)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .build();
          jmxReporter.start();
          break;
      }
    }
  }

  private <T, V> void addGauge(Class<T> tClass, String gaugeName, Gauge<V> gauge) {
    SentryMetricsServletContextListener.METRIC_REGISTRY.register(
        MetricRegistry.name(tClass, gaugeName), gauge);
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

  public enum Reporting {
    JMX,
    CONSOLE;
  }
}