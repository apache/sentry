/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.sentry.provider.db.generic;

import com.google.common.collect.Table;
import com.google.common.collect.HashBasedTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.provider.common.TableCache;
import org.apache.sentry.provider.db.generic.service.thrift.*;
import org.apache.sentry.provider.db.generic.tools.TSentryPrivilegeConverter;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public final class UpdatableCache implements TableCache, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpdatableCache.class);

  // Timer for getting updates periodically
  private final Timer timer = new Timer();
  private boolean initialized = false;
  // saved timer is used by tests to cancel previous timer
  private static Timer savedTimer;

  private final String componentType;
  private final String serviceName;
  private final long cacheTtlNs;
  private final int allowedUpdateFailuresCount;
  private final Configuration conf;
  private final TSentryPrivilegeConverter tSentryPrivilegeConverter;

  private volatile long lastRefreshedNs = 0;
  private int consecutiveUpdateFailuresCount = 0;
  /**
   * Sparse table where group is the row key and role is the cell.
   * The value is the set of privileges located in the cell. For example,
   * the following table would be generated for a policy where Group 1
   * has Role 1 and Role 2 while Group 2 has only Role 2.
   * <table border="1">
   *  <tbody>
   *    <tr>
   *      <td><!-- empty --></td>
   *      <td>Role 1</td>
   *      <td>Role 2</td>
   *    </tr>
   *    <tr>
   *      <td>Group 1</td>
   *      <td>Priv 1</td>
   *      <td>Priv 2, Priv 3</td>
   *    </tr>
   *    <tr>
   *      <td>Group 2</td>
   *      <td><!-- empty --></td>
   *      <td>Priv 2, Priv 3</td>
   *    </tr>
   *  </tbody>
   * </table>
   */
  private volatile Table<String, String, Set<String>> table;

  UpdatableCache(Configuration conf, String componentType, String serviceName, TSentryPrivilegeConverter tSentryPrivilegeConverter) {
    this.conf = conf;
    this.componentType = componentType;
    this.serviceName = serviceName;
    this.tSentryPrivilegeConverter = tSentryPrivilegeConverter;

    // check caching configuration
    this.cacheTtlNs = TimeUnit.MILLISECONDS.toNanos(conf.getLong(ServiceConstants.ClientConfig.CACHE_TTL_MS, ServiceConstants.ClientConfig.CACHING_TTL_MS_DEFAULT));
    this.allowedUpdateFailuresCount = conf.getInt(ServiceConstants.ClientConfig.CACHE_UPDATE_FAILURES_BEFORE_PRIV_REVOKE, ServiceConstants.ClientConfig.CACHE_UPDATE_FAILURES_BEFORE_PRIV_REVOKE_DEFAULT);
  }

  @Override
  public Table<String, String, Set<String>> getCache() {
    return table;
  }

  /**
   * Build cache replica with latest values
   *
   * @return cache replica with latest values
   */
  private Table<String, String, Set<String>> loadFromRemote() throws Exception {
    Table<String, String, Set<String>> tempCache = HashBasedTable.create();
    String requestor;
    requestor = UserGroupInformation.getLoginUser().getShortUserName();

    try(SentryGenericServiceClient client = getClient()) {
      Set<TSentryRole>  tSentryRoles = client.listAllRoles(requestor, componentType);

      for (TSentryRole tSentryRole : tSentryRoles) {
        final String roleName = tSentryRole.getRoleName();
        final Set<TSentryPrivilege> tSentryPrivileges =
                client.listAllPrivilegesByRoleName(requestor, roleName, componentType, serviceName);
        for (String group : tSentryRole.getGroups()) {
          Set<String> currentPrivileges = tempCache.get(group, roleName);
          if (currentPrivileges == null) {
            currentPrivileges = new HashSet<>();
            tempCache.put(group, roleName, currentPrivileges);
          }
          for (TSentryPrivilege tSentryPrivilege : tSentryPrivileges) {
            currentPrivileges.add(tSentryPrivilegeConverter.toString(tSentryPrivilege));
          }
        }
      }
      return tempCache;
    }
  }

  /**
   *  The Sentry-296(generate client for connection pooling) has already finished development and reviewed by now. When it
   *  was committed to master, the getClient method was needed to refactor using the connection pool
   *
   *  TODO: Avoid creating new client each time.
   */
  private SentryGenericServiceClient getClient() throws Exception {
    return SentryGenericServiceClientFactory.create(conf);
  }

  void startUpdateThread(boolean blockUntilFirstReload) throws Exception {
    if (blockUntilFirstReload) {
      reloadData();
    }

    if (initialized) {
      LOGGER.info("Already initialized");
      return;
    }

    initialized = true;
    // Save timer to be able to cancel it.
    if (savedTimer != null) {
      LOGGER.debug("Resetting saved timer");
      savedTimer.cancel();
    }
    savedTimer = timer;

    final long refreshIntervalMs = TimeUnit.NANOSECONDS.toMillis(cacheTtlNs);
    timer.scheduleAtFixedRate(
        new TimerTask() {
          public void run() {
            if (shouldRefresh()) {
              try {
                LOGGER.debug("Loading all data.");
                reloadData();
              } catch (Exception e) {
                LOGGER.warn("Exception while updating data from DB", e);
                revokeAllPrivilegesIfRequired();
              }
            }
          }
        },
        blockUntilFirstReload ? refreshIntervalMs : 0,
        refreshIntervalMs);
  }

  private void revokeAllPrivilegesIfRequired() {
    if (++consecutiveUpdateFailuresCount > allowedUpdateFailuresCount) {
      consecutiveUpdateFailuresCount = 0;
      // Clear cache to revoke all privileges.
      // Update table cache to point to an empty table to avoid thread-unsafe characteristics of HashBasedTable.
      this.table = HashBasedTable.create();
      LOGGER.error("Failed to update roles and privileges cache for " + consecutiveUpdateFailuresCount + " times." +
          " Revoking all privileges from cache, which will cause all authorization requests to fail.");
    }
  }

  private void reloadData() throws Exception {
    this.table = loadFromRemote();
    lastRefreshedNs = System.nanoTime();
  }

  private boolean shouldRefresh() {
    final long currentTimeNs = System.nanoTime();
    return lastRefreshedNs + cacheTtlNs < currentTimeNs;
  }

  /**
   * Only called by tests to disable timer.
   */
  public static void disable() {
    if (savedTimer != null) {
      LOGGER.info("Disabling timer");
      savedTimer.cancel();
    }
  }

  @Override
  public void close() {
    timer.cancel();
    savedTimer = null;
    LOGGER.info("Closed Updatable Cache");
  }
}
