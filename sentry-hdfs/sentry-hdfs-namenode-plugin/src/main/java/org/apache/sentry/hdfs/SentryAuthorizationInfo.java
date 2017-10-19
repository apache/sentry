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

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.util.StringUtils;
import org.apache.sentry.hdfs.Updateable.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class SentryAuthorizationInfo implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(SentryAuthorizationInfo.class);

  private static final String SENTRY_AUTHORIZATION_INFO_THREAD_NAME = "sentry-auth-info-refresher";

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
    String[] newPathPrefixes = conf.getTrimmedStrings(
        SentryAuthorizationConstants.HDFS_PATH_PREFIXES_KEY, 
        SentryAuthorizationConstants.HDFS_PATH_PREFIXES_DEFAULT);
    if (newPathPrefixes.length == 0) {
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

      LOG.info("Sentry authorization will enforced in the following HDFS locations: [{}]",
            StringUtils.arrayToString(newPathPrefixes));

      setPrefixPaths(newPathPrefixes);
      LOG.info("Refresh interval [{}]ms, retry wait [{}]",
          refreshIntervalMillisec, retryWaitMillisec);
      LOG.info("stale threshold [{}]ms", staleThresholdMillisec);

      authzPaths = new UpdateableAuthzPaths(newPathPrefixes);
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
    //Looks like getting same updates multiple times
    SentryAuthzUpdate updates = updater.getUpdates();
    // Updates can be null if Sentry Service is un-reachable
    if (updates != null) {
      if (updates.isEmpty()) {
        return true; // no updates is a norm, it's still success
      }

      UpdateableAuthzPaths newAuthzPaths = processUpdates(
          updates.getPathUpdates(), authzPaths);
      UpdateableAuthzPermissions newAuthzPerms = processUpdates(
          updates.getPermUpdates(), authzPermissions);

      // processUpdates() should return different newAuthzPaths and newAuthzPerms object references
      // if FULL updates were fetched from the Sentry server, otherwise, the same authzPaths and authzPermissions
      // objects will be returned.
      if (newAuthzPaths != authzPaths || newAuthzPerms != authzPermissions) {
        lock.writeLock().lock();
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug(updates.dumpContent());
          }
          if (newAuthzPaths != authzPaths) {
            LOG.info(String.format("FULL Updated paths seq Num [old=%d], [new=%d]",
              authzPaths.getLastUpdatedSeqNum(), newAuthzPaths.getLastUpdatedSeqNum()));
            authzPaths = newAuthzPaths;
            if (LOG.isTraceEnabled()) {
              LOG.trace(authzPaths.dumpContent());
            }
          }
          if (newAuthzPerms != authzPermissions) {
            LOG.info(String.format("FULL Updated perms seq Num [old=%d], [new=%d]",
              authzPermissions.getLastUpdatedSeqNum(), newAuthzPerms.getLastUpdatedSeqNum()));
            authzPermissions = newAuthzPerms;
            if (LOG.isTraceEnabled()) {
              LOG.trace(authzPermissions.dumpContent());
            }
          }
        } finally {
          lock.writeLock().unlock();
        }
      } else {
        if (LOG.isDebugEnabled()) {
          lock.writeLock().lock();
          try {
            LOG.debug(updates.dumpContent());
            if (LOG.isTraceEnabled()) {
              LOG.trace(newAuthzPaths.dumpContent());
              LOG.trace(newAuthzPerms.dumpContent());
            }
          } finally {
            lock.writeLock().unlock();
          }
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
    V newUpdateable = updateable;
    if (!updates.isEmpty()) {
      if (updates.get(0).hasFullImage()) {
        LOG.debug(String.format("Process Update : FULL IMAGE [%s][%d][%d]",
            newUpdateable.getClass().getSimpleName(),
            updates.get(0).getSeqNum(),
            updates.get(0).getImgNum()));
        newUpdateable = (V)newUpdateable.updateFull(updates.remove(0));
      }
      // Any more elements ?
      if (!updates.isEmpty()) {
        LOG.debug(String.format("Process Update : More updates.. [%s][%d][%d][%d]",
            newUpdateable.getClass().getSimpleName(),
            newUpdateable.getLastUpdatedSeqNum(),
            newUpdateable.getLastUpdatedImgNum(),
            updates.size()));
        newUpdateable.updatePartial(updates, lock);
      }
      LOG.debug(String.format("Process Update : Finished updates.. [%s][%d][%d]",
          newUpdateable.getClass().getSimpleName(),
          newUpdateable.getLastUpdatedSeqNum(),
          newUpdateable.getLastUpdatedImgNum()));
    }
    return newUpdateable;
  }

  @Override
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
    if (authzPaths != null || authzPermissions != null) {
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
      ThreadFactory sentryAuthInfoRefresherThreadFactory = new ThreadFactoryBuilder()
          .setNameFormat(SENTRY_AUTHORIZATION_INFO_THREAD_NAME)
          .setDaemon(true)
          .build();
      executor = Executors.newSingleThreadScheduledExecutor(sentryAuthInfoRefresherThreadFactory);
      executor.scheduleWithFixedDelay(this, refreshIntervalMillisec, 
          refreshIntervalMillisec, TimeUnit.MILLISECONDS);
    }
  }

  public void stop() {
    if (authzPaths != null) {
      LOG.info(getClass().getSimpleName() + ": Stopping");
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

  public boolean isUnderPrefix(String[] pathElements) {
    lock.readLock().lock();
    try {
      return authzPaths.isUnderPrefix(pathElements);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Deprecated
  public boolean isManaged(String[] pathElements) {
    return isUnderPrefix(pathElements);
  }

  public boolean doesBelongToAuthzObject(String[] pathElements) {
    lock.readLock().lock();
    try {
      return authzPaths.findAuthzObject(pathElements) != null;
    } finally {
      lock.readLock().unlock();
    }
  }

  public boolean isSentryManaged(final String[] pathElements) {
    lock.readLock().lock();
    try {
      return authzPaths.isUnderPrefix(pathElements) &&
          authzPaths.findAuthzObject(pathElements) != null;
    } finally {
      lock.readLock().unlock();
    }
  }

  @SuppressWarnings("unchecked")
  public List<AclEntry> getAclEntries(String[] pathElements) {
    lock.readLock().lock();
    try {
      Set<String> authzObjs = authzPaths.findAuthzObject(pathElements);
      // Apparently setFAcl throws error if 'group::---' is not present
      AclEntry noGroup = AclEntry.parseAclEntry("group::---", true);

      Set<AclEntry> retSet = new HashSet<>();
      retSet.add(noGroup);

      if (authzObjs == null) {
        retSet.addAll(Collections.<AclEntry>emptyList());
        return new ArrayList<>(retSet);
      }

      // No duplicate acls should be added.
      for (String authzObj: authzObjs) {
        retSet.addAll(authzPermissions.getAcls(authzObj));
      }

      return new ArrayList<>(retSet);
    } finally {
      lock.readLock().unlock();
    }
  }

}
