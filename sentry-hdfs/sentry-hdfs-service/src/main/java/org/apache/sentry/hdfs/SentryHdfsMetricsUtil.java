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
final class SentryHdfsMetricsUtil {
  // SentryMetrics
  private static final SentryMetrics sentryMetrics = SentryMetrics.getInstance();

  // Metrics for get_all_authz_updates_from in SentryHDFSServiceProcessor
  // The time used for each get_all_authz_updates_from
  static final Timer getAllAuthzUpdatesTimer = sentryMetrics.getTimer(
      MetricRegistry.name(SentryHDFSServiceProcessor.class,
          "get-all-authz-updates-from"));
  // The size of perm updates for each get_all_authz_updates_from
  static final Histogram getPermUpdateHistogram = sentryMetrics.getHistogram(
      MetricRegistry.name(SentryHDFSServiceProcessor.class, "perm-updates-size"));
  // The size of path updates for each get_all_authz_updates_from
  static final Histogram getPathUpdateHistogram = sentryMetrics.getHistogram(
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

  // Metrics for retrievePermFullImage in PermImageRetriever
  // The time used for each retrievePermFullImage
  static final Timer getRetrievePermFullImageTimer = sentryMetrics.getTimer(
      MetricRegistry.name(PermImageRetriever.class, "retrieve-perm-full-image"));
  // The size of privilege changes for each retrievePermFullImage
  static final Histogram getPrivilegeChangesHistogram = sentryMetrics.getHistogram(
      MetricRegistry.name(PermImageRetriever.class, "retrieve-perm-full-image",
          "privilege-changes-size"));
  // The size of role changes for each retrievePermFullImage call
  static final Histogram getRoleChangesHistogram = sentryMetrics.getHistogram(
      MetricRegistry.name(PermImageRetriever.class, "retrieve-perm-full-image",
          "role-changes-size"));

  // Metrics for retrievePathFullImage in PathImageRetriever
  // The time used for each retrievePathFullImage
  static final Timer getRetrievePathFullImageTimer = sentryMetrics.getTimer(
      MetricRegistry.name(PathImageRetriever.class, "retrieve-path-full-image"));

  // The size of path changes for each retrievePathFullImage
  static final Histogram getPathChangesHistogram = sentryMetrics.getHistogram(
      MetricRegistry.name(PathImageRetriever.class, "retrieve-path-full-image",
          "path-changes-size"));

  // Timer for getting path changes deltas
  static final Timer getDeltaPathChangesTimer = sentryMetrics.getTimer(
    MetricRegistry.name(PathDeltaRetriever.class, "path", "delta", "time")
  );

  // Histogram for the number of path changes processed for deltas
  static final Histogram getDeltaPathChangesHistogram = sentryMetrics.getHistogram(
          MetricRegistry.name(PathDeltaRetriever.class, "path", "delta", "size"));


  // Timer for getting permission changes deltas
  static final Timer getDeltaPermChangesTimer = sentryMetrics.getTimer(
          MetricRegistry.name(PathDeltaRetriever.class, "perm", "delta", "time")
  );

  // Histogram for the number of permissions changes processed for deltas
  static final Histogram getDeltaPermChangesHistogram = sentryMetrics.getHistogram(
          MetricRegistry.name(PathDeltaRetriever.class, "perm", "delta", "size"));

  private SentryHdfsMetricsUtil() {
    // Make constructor private to avoid instantiation
  }
}
