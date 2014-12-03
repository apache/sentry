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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.util.StringUtils;
import org.apache.sentry.hdfs.SentryHDFSServiceClient.SentryAuthzUpdate;
import org.apache.sentry.hdfs.Updateable.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SentryAuthorizationInfo implements Runnable {
  private static Logger LOG =
      LoggerFactory.getLogger(SentryAuthorizationInfo.class);

  private SentryUpdater updater;
  private volatile UpdateableAuthzPaths authzPaths;
  private volatile UpdateableAuthzPermissions authzPermissions;

  private int refreshIntervalMillisec;
  private int staleThresholdMillisec;
  private int retryWaitMillisec;
  private ScheduledExecutorService executor;
  private volatile long lastUpdate;
  private volatile long waitUntil;
  private volatile long lastStaleReport;
  // We don't need a re-entrant lock.. but we do need a ReadWriteLock
  // Unfortunately, the ReentrantReadWriteLick is the only available
  // concrete implementation of a ReadWriteLock.
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private String[][] pathPrefixes;

  // For use only for testing !!
  @VisibleForTesting
  SentryAuthorizationInfo(String[] pathPrefixes) {
    setPrefixPaths(pathPrefixes);
  }

  public SentryAuthorizationInfo(Configuration conf) throws Exception {
    String[] pathPrefixes = conf.getTrimmedStrings(
        SentryAuthorizationConstants.HDFS_PATH_PREFIXES_KEY, 
        SentryAuthorizationConstants.HDFS_PATH_PREFIXES_DEFAULT);
    if (pathPrefixes.length == 0) {
      LOG.warn("There are not HDFS path prefixes configured in [{}], "
          + "Sentry authorization won't be enforced on any HDFS location",
          SentryAuthorizationConstants.HDFS_PATH_PREFIXES_KEY);
    } else {
      refreshIntervalMillisec = conf.getInt(
          SentryAuthorizationConstants.CACHE_REFRESH_INTERVAL_KEY,
          SentryAuthorizationConstants.CACHE_REFRESH_INTERVAL_DEFAULT);
      staleThresholdMillisec = conf.getInt(
          SentryAuthorizationConstants.CACHE_STALE_THRESHOLD_KEY,
          SentryAuthorizationConstants.CACHE_STALE_THRESHOLD_DEFAULT);
      retryWaitMillisec = conf.getInt(
          SentryAuthorizationConstants.CACHE_REFRESH_RETRY_WAIT_KEY,
          SentryAuthorizationConstants.CACHE_REFRESH_RETRY_WAIT_DEFAULT);

      LOG.debug("Sentry authorization will enforced in the following HDFS " +
          "locations: [{}]", StringUtils.arrayToString(pathPrefixes));
      setPrefixPaths(pathPrefixes);
      LOG.debug("Refresh interval [{}]ms, retry wait [{}], stale threshold " +
              "[{}]ms", new Object[] 
          {refreshIntervalMillisec, retryWaitMillisec, staleThresholdMillisec});

      authzPaths = new UpdateableAuthzPaths(pathPrefixes);
      authzPermissions = new UpdateableAuthzPermissions();
      waitUntil = System.currentTimeMillis();
      lastStaleReport = 0;
      updater = new SentryUpdater(conf, this);
    }
  }

  private void setPrefixPaths(String[] pathPrefixes) {
    this.pathPrefixes = new String[pathPrefixes.length][];
    for (int i = 0; i < this.pathPrefixes.length; i++) {
      Preconditions.checkArgument(
          pathPrefixes[i].startsWith("" + Path.SEPARATOR_CHAR),
          "Path prefix [" + pathPrefixes[i] + "]"
              + "does not starting with [" + Path.SEPARATOR_CHAR + "]");
      this.pathPrefixes[i] =
          pathPrefixes[i].substring(1).split("" + Path.SEPARATOR_CHAR);
    }
  }

  String[][] getPathPrefixes() {
    return pathPrefixes;
  }

  UpdateableAuthzPaths getAuthzPaths() {
    return authzPaths;
  }

  UpdateableAuthzPermissions getAuthzPermissions() {
    return authzPermissions;
  }

  private boolean update() {
    SentryAuthzUpdate updates = updater.getUpdates();
    // Updates can be null if Sentry Service is un-reachable
    if (updates != null) {
      UpdateableAuthzPaths newAuthzPaths = processUpdates(
          updates.getPathUpdates(), authzPaths);
      UpdateableAuthzPermissions newAuthzPerms = processUpdates(
          updates.getPermUpdates(), authzPermissions);
      // If there were any FULL updates the returned instance would be
      // different
      if ((newAuthzPaths != authzPaths)||(newAuthzPerms != authzPermissions)) {
        lock.writeLock().lock();
        try {
          authzPaths = newAuthzPaths;
          if (LOG.isDebugEnabled()) {
            LOG.debug("FULL Updated paths seq Num [" + authzPaths.getLastUpdatedSeqNum() + "]");
          }
          authzPermissions = newAuthzPerms;
          if (LOG.isDebugEnabled()) {
            LOG.debug("FULL Updated perms seq Num [" + authzPermissions.getLastUpdatedSeqNum() + "]");
          }
        } finally {
          lock.writeLock().unlock();
        }
      }
      return true;
    }
    return false;
  }

  private <K extends Update, V extends Updateable<K>> V processUpdates(List<K> updates,
      V updateable) {
    // In a list of Updates, if there is a full Update, it will be the first
    // one in the List.. all the remaining will be partial updates
    if (updates.size() > 0) {
      if (updates.get(0).hasFullImage()) {
        updateable = (V)updateable.updateFull(updates.remove(0));
      }
      // Any more elements ?
      if (!updates.isEmpty()) {
        updateable.updatePartial(updates, lock);
      }
    }
    return updateable;
  }

  public void run() {
    boolean success = false;
    try {
      // In case of previous preUpdate failure, we sleep for a retry wait 
      // interval we can do this because we are using a singledthreadedexecutor
      // and scheduling the runs with fixed delay.
      long currTime = System.currentTimeMillis();
      if (waitUntil > currTime) {
        Thread.sleep(waitUntil - currTime);
      }
      success = update();
    } catch (Exception ex) {
      success = false;
      LOG.warn("Failed to update, will retry in [{}]ms, error: ", 
          new Object[]{ retryWaitMillisec, ex.getMessage(), ex});
    }
    if (success) {
      // we reset lastUpdate only on successful pulling
      lastUpdate = System.currentTimeMillis();
      waitUntil = lastUpdate;
    } else {
      waitUntil = System.currentTimeMillis() + retryWaitMillisec;
    }
  }

  public void start() {
    if (authzPaths != null) {
      boolean success = false;
      try {
        success = update();
      } catch (Exception ex) {
        success = false;
        LOG.warn("Failed to do initial update, will retry in [{}]ms, error: ",
            new Object[]{retryWaitMillisec, ex.getMessage(), ex});
      }
      if (!success) {
        waitUntil = System.currentTimeMillis() + retryWaitMillisec;
      }
      executor = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
              Thread t = new Thread(r, SentryAuthorizationInfo.class.getName() +
                  "-refresher");
              t.setDaemon(true);
              return t;
            }
          }
      );
      executor.scheduleWithFixedDelay(this, refreshIntervalMillisec, 
          refreshIntervalMillisec, TimeUnit.MILLISECONDS);
    }
  }

  public void stop() {
    if (authzPaths != null) {
      executor.shutdownNow();
    }
  }

  public boolean isStale() {
    long now = System.currentTimeMillis();
    boolean stale = now - lastUpdate > staleThresholdMillisec;
    if (stale && now - lastStaleReport > retryWaitMillisec) {
      LOG.warn("Authorization information has been stale for [{}]s", 
          (now - lastUpdate) / 1000);
      lastStaleReport = now;
    }
    return stale;
  }

  public boolean isManaged(String[] pathElements) {
    lock.readLock().lock();
    try {
      return authzPaths.isUnderPrefix(pathElements);
    } finally {
      lock.readLock().unlock();
    }
  }

  public boolean doesBelongToAuthzObject(String[] pathElements) {
    lock.readLock().lock();
    try {
      return authzPaths.findAuthzObject(pathElements) != null;
    } finally {
      lock.readLock().unlock();
    }
  }

  @SuppressWarnings("unchecked")
  public List<AclEntry> getAclEntries(String[] pathElements) {
    lock.readLock().lock();
    try {
      String authzObj = authzPaths.findAuthzObject(pathElements);
      // Apparently setFAcl throws error if 'group::---' is not present
      AclEntry noGroup = AclEntry.parseAclEntry("group::---", true);
      ArrayList<AclEntry> retList = Lists.newArrayList(noGroup);
      retList.addAll((authzObj != null) ? authzPermissions.getAcls(authzObj)
          : Collections.EMPTY_LIST);
      return retList;
    } finally {
      lock.readLock().unlock();
    }
  }

}
