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

package org.apache.sentry.hdfs;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.sentry.provider.db.service.thrift.SentryMetrics;

/**
 * Util class to support metrics.
 */
public class SentryHdfsMetricsUtil {
  // SentryMetrics
  private static final SentryMetrics sentryMetrics = SentryMetrics.getInstance();

  // Metrics for get_all_authz_updates_from in SentryHDFSServiceProcessor
  // The time used for each get_all_authz_updates_from
  public static final Timer getAllAuthzUpdatesTimer = sentryMetrics.getTimer(
      MetricRegistry.name(SentryHDFSServiceProcessor.class,
          "get-all-authz-updates-from"));
  // The size of perm updates for each get_all_authz_updates_from
  public static final Histogram getPermUpdateHistogram = sentryMetrics.getHistogram(
      MetricRegistry.name(SentryHDFSServiceProcessor.class, "perm-updates-size"));
  // The size of path updates for each get_all_authz_updates_from
  public static final Histogram getPathUpdateHistogram = sentryMetrics.getHistogram(
      MetricRegistry.name(SentryHDFSServiceProcessor.class, "paths-updates-size"));

  // Metrics for handle_hms_notification in SentryHDFSServiceProcessor
  // The time used for each handle_hms_notification
  public static final Timer getHandleHmsNotificationTimer = sentryMetrics.getTimer(
      MetricRegistry.name(SentryHDFSServiceProcessor.class, "handle-hms-notification"));
  // The number of failed handle_hms_notification
  public static final Counter getFailedHandleHmsNotificationCounter =
      sentryMetrics.getCounter(MetricRegistry.name(SentryHDFSServiceProcessor.class,
          "handle-hms-notification", "failed-num"));
  // The number of handle_hms_notification with full image update
  public static final Counter getHandleHmsHasFullImageCounter = sentryMetrics.getCounter(
      MetricRegistry.name(SentryHDFSServiceProcessor.class, "handle-hms-notification",
          "has-full-image-num"));
  // The size of path changes for each handle_hms_notification
  public static final Histogram getHandleHmsPathChangeHistogram = sentryMetrics.getHistogram(
      MetricRegistry.name(SentryHDFSServiceProcessor.class, "handle-hms-notification",
          "path-changes-size"));

  // Metrics for retrieveFullImage in SentryPlugin.PermImageRetriever
  // The time used for each retrieveFullImage
  public static final Timer getRetrieveFullImageTimer = sentryMetrics.getTimer(
      MetricRegistry.name(SentryPlugin.PermImageRetriever.class, "retrieve-full-image"));
  // The size of privilege changes for each retrieveFullImage
  public static final Histogram getPrivilegeChangesHistogram = sentryMetrics.getHistogram(
      MetricRegistry.name(SentryPlugin.PermImageRetriever.class, "retrieve-full-image",
          "privilege-changes-size"));
  // The size of role changes for each retrieveFullImage call
  public static final Histogram getRoleChangesHistogram = sentryMetrics.getHistogram(
      MetricRegistry.name(SentryPlugin.PermImageRetriever.class, "retrieve-full-image",
          "role-changes-size"));

  // Metrics for notifySentry HMS update in MetaStorePlugin
  // The timer used for each notifySentry
  public static final Timer getNotifyHMSUpdateTimer = sentryMetrics.getTimer(
      MetricRegistry.name(MetastorePlugin.class, "notify-sentry-HMS-update"));
  // The number of failed notifySentry
  public static final Counter getFailedNotifyHMSUpdateCounter = sentryMetrics.getCounter(
      MetricRegistry.name(MetastorePlugin.class, "notify-sentry-HMS-update",
          "failed-num"));

  // Metrics for applyLocal update in MetastorePlugin
  // The time used for each applyLocal
  public static final Timer getApplyLocalUpdateTimer = sentryMetrics.getTimer(
      MetricRegistry.name(MetastorePlugin.class, "apply-local-update"));
  // The size of path changes for each applyLocal
  public static final Histogram getApplyLocalUpdateHistogram = sentryMetrics.getHistogram(
      MetricRegistry.name(MetastorePlugin.class, "apply-local-update",
          "path-change-size"));

  // Metrics for handleCacheUpdate to ZK in PluginCacheSyncUtil
  // The time used for each handleCacheUpdate
  public static final Timer getCacheSyncToZKTimer = sentryMetrics.getTimer(
      MetricRegistry.name(PluginCacheSyncUtil.class, "cache-sync-to-zk"));
  // The number of failed handleCacheUpdate
  public static final Counter getFailedCacheSyncToZK = sentryMetrics.getCounter(
      MetricRegistry.name(PluginCacheSyncUtil.class, "cache-sync-to-zk", "failed-num"));
  
  private SentryHdfsMetricsUtil() {
    // Make constructor private to avoid instantiation
  }
}
