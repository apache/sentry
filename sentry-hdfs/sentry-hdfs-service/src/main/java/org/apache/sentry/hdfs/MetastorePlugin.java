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

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.sentry.hdfs.ServiceConstants.ServerConfig;
import org.apache.sentry.provider.db.SentryMetastoreListenerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Plugin for the components that need to send path creation, update, and deletion
 * notifications to the Sentry daemon.
 *
 * <p>
 * Implements {@link SentryMetastoreListenerPlugin} that hooks
 * into the sites in the {@link MetaStorePreEventListener}.
 *
 * <p>
 * Implementation Notes:
 *
 * <ol>
 * <li>MetastorePlugin performs the following functions:
 *
 * <ul>
 *  <li> At the construction time:
 *   <ul>
 *    <li> Initializes local HMS cache with HMS paths information.
 *    <li> Sends initial HMS paths information to the Sentry daemon.
 *   </ul>
 *  </li>
 *  <li> Upon receiving path update notification from the hosting client code, via addPath(),
 *       removePath(), removeAllPaths(), and renameAuthzObject() callback methods:
 *   <ul>
 *    <li> Updates local HMS cache accordingly.
 *    <li> Sends partial update with the assigned sequence number to the Sentry daemon.
 *    <li> Maintains the latest Sentry partial update sequence number, incrementing it by 1 on each update.
 *   </ul>
 *  </li>
 *  <li> Periodically, from the housekeeping thread:
 *   <ul>
 *    <li> Contacts the Sentry daemon to ask for the sequence number of the latest received update.
 *    <li> If the sequence number returned by the Sentry daemon does not match the sequence number of the
 *         latest update sent from MetastorePlugin, send the full HMS paths image to the Sentry daemon.
 *   </ul>
 *  </li>
 * </ul>
 *
 * <p>
 * <li>MetastorePlugin must be a singleton.<br>
 * Only a single instance of MetastorePlugin can be used. MetastorePlugin has HMS cache
 * that is updated via calling addPath(), removePath(), removeAllPaths(), renameAuthzObject().
 * This cache must represent full HMS state at any point, so that full updates, when they are
 * needed, would be correct. Channelling different update requests through different MetastorePlugin
 * instances would make those caches partial and mutually inconsistent.
 *
 * <p>
 * <li>MetastorePlugin is always created, even though ininitialization may fail.<br>
 * MetastorePlugin initialization (object construction) may fail for two reasons:
 * <ul>
 *  <li> HMS cache cannot be initialized, usually due to some invalid HMS path entries.
 *  <li> Initial cache cannot be sent to Sentry, e.g. due to the communication problems.
 * </ul>
 *
 * <p>
 * In either case, MetastorePlugin is still constructed, in consideration with the design of
 * the existing client code. However, such an instance is marked as invalid; all update APIs
 * throw IllegalStateException with the appropriate error message and root cause exception.
 * <br>TODO: failing to construct MetastorePlugin on initialization failure would be much cleaner,
 *       but it has to be done in coordination with the HMS client code.
 *
 * <p>
 * <li>MetastorePlugin guarantees delivery of HMS paths updates to Sentry daemon in the right order.<br>
 * Each invocation of addPath(), removePath(), removeAllPaths(), renameAuthzObject()
 * triggers two actions:
 * <ul>
 *  <li> increment update sequence number and update the local cache and
 *  <li> send partial update to the Sentry daemon.
 * </ul>
 *
 * <p>
 * Update sequence number is created at first step, and then it travels as part of the update information,
 * to the Sentry daemon on the second step. Therefore, the sequence of both steps must be
 * atomic, to guarantee that updates arrive to the Sentry daemon in the right order,
 * with sequential update number. This is achieved by using notificationLock. The same lock is used
 * inside the SyncTask during full Sentry update, when the local and Sentry-side update sequence
 * numbers are out of sync.
 *
 * <p>
 * <li>MetastorePlugin validates input paths.<br>
 * Parsing malformed input paths generates SentryMalformedPathException. Since this is a checked
 * exception, it is re-thrown wrapped into (un-checked) IllegalArgumentException, to preserve
 * public APIs' signatures.
 *
 * </ol>
 */

public class MetastorePlugin extends SentryMetastoreListenerPlugin {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetastorePlugin.class);

  /* MetastorePlugin initialization may fail for two different reasons:
   * a) Failure to initialize HMS paths cache.
   * b) Failure to send the initial HMS paths to the Sentry daemon.
   * Each of the two messages below conveys the reason.
   */
  private static final String CACHE_INIT_FAILURE_MSG =
    "Cache failed to initialize, cannot send path updates to Sentry." +
    " Please review HMS error logs during startup for additional information. If the initialization failure is due" +
    " to SentryMalformedPathException, you will need to rectify the malformed path in HMS db and restart HMS";
  private static final String SENTRY_INIT_UPDATE_FAILURE_MSG =
    "Metastore Plugin failed to initialize - cannot send initial HMS updates to Sentry";

  private static final String SENTRY_COMM_FAILURE_MSG = "Cannot Communicate with Sentry";

  private final Configuration conf;
  private final Configuration sentryConf;

  // guard for all local+Sentry notifications
  private final ReentrantLock notificationLock = new ReentrantLock();
  // sentryClient may be re-instantiated in case of suspected communication failure
  // This code ensures that access to sentryClient is protected by notificationLock
  private SentryHDFSServiceClient sentryClient;
  // Has to match the value of seqNum
  // This code ensures that access to lastSentSeqNum is protected by notificationLock
  protected long lastSentSeqNum;

  // pathUpdateLock guards access to UpdateableAuthzPaths which is not thread-safe
  private final ReentrantReadWriteLock pathUpdateLock = new ReentrantReadWriteLock();
  // access to authzPaths must be protected by pathUpdateLock
  private final UpdateableAuthzPaths authzPaths;

  // Initialized to some value > 1.
  protected final AtomicLong seqNum = new AtomicLong(5);
  private final Throwable initError;
  private final String initErrorMsg;
  private final ScheduledExecutorService threadPool; //NOPMD

  private static volatile ScheduledExecutorService lastThreadPool = null;

  /*
   * This task is scheduled to run periodically, to make sure Sentry has all updates
   * -- only if MetastorePlugin has been successfully initialized.
   */
  class SyncTask implements Runnable {
    @Override
    public void run() {
      if (!notificationLock.tryLock()) {
        // No need to sync.. as metastore is in the process of pushing an update..
        return;
      }
      try {
        long lastSeenBySentry = getLastSeenHMSPathSeqNum();
        long lastSent = lastSentSeqNum;
        if (lastSeenBySentry != lastSent) {
          LOGGER.warn("#### Sentry not in sync with HMS [" + lastSeenBySentry + ", "
              + lastSent + "]");
          notifySentryFullUpdate(lastSent);
        }
      } catch (Exception ignore) {
        // all methods inside try {} log errors anyway
      } finally {
        notificationLock.unlock();
      }
    }
  }

  /*
   * Proxy class for RPC calls to the Sentry daemon
   */
  static class ProxyHMSHandler extends HMSHandler {
    public ProxyHMSHandler(String name, HiveConf conf) throws MetaException {
      super(name, conf);
    }
  }

  private void updateSentryConfFromHiveConf(String property) {
    if (sentryConf.get(property) == null || sentryConf.get(property).isEmpty()) {
      String value = conf.get(property);
      if(value != null) {
        LOGGER.warn("Deprecated: Property " + property + " was set in hive-site.xml, " +
            "it should be instead set in sentry-site.xml");
        sentryConf.set(property, value);
      }
    }
  }

  @VisibleForTesting
  public Configuration getSentryConf() {
    return sentryConf;
  }

  /*
   * Test-only logic. Testing framework may create multiple MetastorePlugin
   * instances in sequence, without explicitly shutting down the previous
   * instance, which does not even have any shutdown API (obvious oversight).
   * This results in multiple housekeeping thread pools, completely messing
   * up HMS state on Sentry daemon.
   * Previous thread pool must be shut down.
   * In real deployments this code does nothing, because there is only one
   * instance of MetastorePlugin.
   */
  private static synchronized void shutdownPreviousHousekeepingThreadPool() {
    if (lastThreadPool != null) {
      LOGGER.info("#### Metastore Plugin: shutting down previous housekeeping thread");
      try {
        lastThreadPool.shutdownNow();
      } catch (Throwable t) {
        LOGGER.error("#### Metastore Plugin: failure shutting down previous housekeeping thread", t);
      }
      lastThreadPool = null;
    }
  }

  public MetastorePlugin(Configuration conf, Configuration sentryConf) {
    Preconditions.checkNotNull(conf, "NULL Hive Configuration");
    Preconditions.checkNotNull(sentryConf, "NULL Sentry Configuration");
    if (!(conf instanceof HiveConf)) {
        String error = "Hive Configuration is not an instanceof HiveConf: " + conf.getClass().getName();
        LOGGER.error(error);
        throw new IllegalArgumentException(error);
    }

    /*
     * Test-only logic. See javadoc for this method.
     */
    shutdownPreviousHousekeepingThreadPool();

    this.conf = new HiveConf((HiveConf)conf);
    this.sentryConf = new Configuration(sentryConf);

    //TODO: Remove this in C6.
    //CDH-28899: Maintain backward compatibility. Allow reading the configs from hive-site
    // along with sentry-site, where sentry-site takes the precedence
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.SERVER_RPC_ADDRESS);
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.SERVER_RPC_PORT);
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.SERVER_RPC_CONN_TIMEOUT);
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.SECURITY_MODE);
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.SECURITY_USE_UGI_TRANSPORT);
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.PRINCIPAL);
    updateSentryConfFromHiveConf(ServiceConstants.ClientConfig.USE_COMPACT_TRANSPORT);

    this.conf.unset(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname);
    this.conf.unset(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.varname);
    this.conf.unset(HiveConf.ConfVars.METASTORE_END_FUNCTION_LISTENERS.varname);
    this.conf.unset(HiveConf.ConfVars.METASTOREURIS.varname);

    Throwable tmpInitError = null;
    String tmpInitErrorMsg = null;

    /* Initialization Step #1: initialize local HMS state cache.
     * To preserve the contract with the existing Hive client code,
     * MetastorePlugin shall be constructed even if initialization fails,
     * though it will be completely unoperable.
     */
    UpdateableAuthzPaths tmpAuthzPaths;
    try (MetastoreCacheInitializer cacheInitializer = new MetastoreCacheInitializer(
        new ProxyHMSHandler("sentry.hdfs", (HiveConf) this.conf),
        this.conf))
    {
      // initialize HMS cache.
      tmpAuthzPaths = cacheInitializer.createInitialUpdate();
      LOGGER.info("#### Metastore Plugin HMS cache initialization complete");
    } catch (Throwable e) {
      tmpInitError = e;
      tmpInitErrorMsg = CACHE_INIT_FAILURE_MSG;
      tmpAuthzPaths = null;
      LOGGER.error("#### " + tmpInitErrorMsg, e);
      for (Throwable thr : e.getSuppressed()) {
        LOGGER.warn("#### Exception while closing cacheInitializer", thr);
      }
    }
    this.authzPaths = tmpAuthzPaths;

    /* If HMS cache initialization failed, further initialization shall be skipped.
     * MetastorePlugin is considered non-operational, and all of its public APIs
     * shall be throwing an exception.
     */
    if (tmpInitError != null) {
      this.threadPool = null;
      this.initError = tmpInitError;
      this.initErrorMsg = tmpInitErrorMsg;
      return;
    }

    /* Initialization Step #2: push initial HMS state to Sentry.
     * Synchronization by notificationLock is for visibility of changes to sentryClient.
     */
    notificationLock.lock();
    try {
      this.lastSentSeqNum = seqNum.get();
      notifySentryFullUpdate(lastSentSeqNum);
      LOGGER.info("#### Metastore Plugin Sentry full initial update complete");
    } catch (Throwable e) {
      tmpInitError = e;
      tmpInitErrorMsg = SENTRY_INIT_UPDATE_FAILURE_MSG;
      LOGGER.error("#### " + tmpInitErrorMsg, e);
    } finally {
      notificationLock.unlock();
    }

    this.initError = tmpInitError;
    this.initErrorMsg = tmpInitErrorMsg;

    /* If sending HMS state to Sentry failed, further initialization shall be skipped.
     * MetastorePlugin is considered non-operational, and all of its public APIs
     * shall be throwing an exception.
     */
    if (this.initError != null) {
      this.threadPool = null;
      return;
    }

    /* Initialization Step #3: schedulle SyncTask to run periodically, to make
     * sure Sentry has the current HMS state.
     */
    this.threadPool = Executors.newScheduledThreadPool(1);
    this.threadPool.scheduleWithFixedDelay(new SyncTask(),
      this.conf.getLong(ServerConfig.SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_MS,
                        ServerConfig.SENTRY_HDFS_INIT_UPDATE_RETRY_DELAY_DEFAULT),
      this.conf.getLong(ServerConfig.SENTRY_HDFS_SYNC_CHECKER_PERIOD_MS,
                        ServerConfig.SENTRY_HDFS_SYNC_CHECKER_PERIOD_DEFAULT),
      TimeUnit.MILLISECONDS);
    MetastorePlugin.lastThreadPool = this.threadPool;
    LOGGER.info("#### Metastore Plugin Sentry initialization complete");
  }

  @Override
  public void addPath(String authzObj, String path) {
    assertInit();

    // validate / parse inputs
    List<String> pathTree = null;
    try {
      pathTree = PathsUpdate.parsePath(path);
    } catch (SentryMalformedPathException e) {
      String err = "Unexpected path in addPath: authzObj = " + authzObj + " , path = " + path;
      LOGGER.error(err, e);
      throw new IllegalArgumentException(err, e);
    }
    if(pathTree == null) {
      LOGGER.debug("#### HMS Path Update ["
        + "OP : addPath, "
        + "authzObj : " + authzObj.toLowerCase() + ", "
        + "path : " + path + "] - nothing to add");
      return;
    }
    LOGGER.debug("#### HMS Path Update ["
        + "OP : addPath, "
        + "authzObj : " + authzObj.toLowerCase() + ", "
        + "path : " + path + "]");

    // do local and remote updates
    notificationLock.lock();
    try {
      PathsUpdate update = createHMSUpdate();
      update.newPathChange(authzObj.toLowerCase()).addToAddPaths(pathTree);
      updateLocalCacheAndNotifySentry(update);
    } finally {
      notificationLock.unlock();
    }
  }

  @Override
  public void removeAllPaths(String authzObj, List<String> childObjects) {
    assertInit();

    // validate / parse inputs
    LOGGER.debug("#### HMS Path Update ["
        + "OP : removeAllPaths, "
        + "authzObj : " + authzObj.toLowerCase() + ", "
        + "childObjs : " + (childObjects == null ? "[]" : childObjects) + "]");

    // do local and remote updates
    notificationLock.lock();
    try {
      PathsUpdate update = createHMSUpdate();
      if (childObjects != null) {
        for (String childObj : childObjects) {
          update.newPathChange(authzObj.toLowerCase() + "." + childObj).addToDelPaths(
            Lists.newArrayList(PathsUpdate.ALL_PATHS));
        }
      }
      update.newPathChange(authzObj.toLowerCase()).addToDelPaths(
            Lists.newArrayList(PathsUpdate.ALL_PATHS));
      updateLocalCacheAndNotifySentry(update);
    } finally {
      notificationLock.unlock();
    }
  }

  @Override
  public void removePath(String authzObj, String path) {
    assertInit();

    // validate / parse inputs
    if ("*".equals(path)) {
      removeAllPaths(authzObj.toLowerCase(), null);
    } else {
      List<String> pathTree = null;
      try {
        pathTree = PathsUpdate.parsePath(path);
      } catch (SentryMalformedPathException e) {
        String err = "Unexpected path in removePath: authzObj = " + authzObj + " , path = " + path;
        LOGGER.error(err, e);
        throw new IllegalArgumentException(err, e);
      }
      if(pathTree == null) {
        LOGGER.debug("#### HMS Path Update ["
          + "OP : removePath, "
          + "authzObj : " + authzObj.toLowerCase() + ", "
          + "path : " + path + "] - nothing to remove");
        return;
      }
      LOGGER.debug("#### HMS Path Update ["
          + "OP : removePath, "
          + "authzObj : " + authzObj.toLowerCase() + ", "
          + "path : " + path + "]");

      // do local and remote updates
      notificationLock.lock();
      try {
        PathsUpdate update = createHMSUpdate();
        update.newPathChange(authzObj.toLowerCase()).addToDelPaths(pathTree);
        updateLocalCacheAndNotifySentry(update);
      } finally {
        notificationLock.unlock();
      }
    }
  }

  @Override
  public void renameAuthzObject(String oldName, String oldPath, String newName,
      String newPath) {
    assertInit();

    // validate / parse inputs
    String oldNameLC = oldName != null ? oldName.toLowerCase() : null;
    String newNameLC = newName != null ? newName.toLowerCase() : null;
    LOGGER.debug("#### HMS Path Update ["
        + "OP : renameAuthzObject, "
        + "oldName : " + oldNameLC + ", "
        + "oldPath : " + oldPath   + ", "
        + "newName : " + newNameLC + ", "
        + "newPath : " + newPath   + "]");

    List<String> newPathTree = null;
    try {
      newPathTree = PathsUpdate.parsePath(newPath);
    } catch (SentryMalformedPathException e) {
      String err = "Unexpected path in renameAuthzObject while parsing newPath: oldName=" + oldName + ", oldPath=" + oldPath +
        ", newName=" + newName + ", newPath=" + newPath;
      LOGGER.error(err, e);
      throw new IllegalArgumentException(err, e);
    }

    List<String> oldPathTree = null;
    try {
      oldPathTree = PathsUpdate.parsePath(oldPath);
    } catch (SentryMalformedPathException e) {
      String err = "Unexpected path in renameAuthzObject while parsing oldPath: oldName=" + oldName + ", oldPath=" + oldPath +
        ", newName=" + newName + ", newPath=" + newPath;
      LOGGER.error(err, e);
      throw new IllegalArgumentException(err, e);
    }

    // do local and remote updates
    notificationLock.lock();
    try {
      PathsUpdate update = createHMSUpdate();
      if( newPathTree != null ) {
        update.newPathChange(newNameLC).addToAddPaths(newPathTree);
      }
      if( oldPathTree != null ) {
        update.newPathChange(oldNameLC).addToDelPaths(oldPathTree);
      }
      updateLocalCacheAndNotifySentry(update);
    } finally {
      notificationLock.unlock();
    }
  }

  /*
   * Instantiate client (unless it's already instantiated) to talk to Sentry service.
   * Call must be protected by notificationLock.
   */
  private SentryHDFSServiceClient getClient() throws Exception {
    assert notificationLock.isHeldByCurrentThread() : "Internal Faulure: access to Sentry client is nt protected by notificationLock";
    if (sentryClient == null) {
      try {
        sentryClient = SentryHDFSServiceClientFactory.create(sentryConf);
      } catch (Exception e) {
        sentryClient = null;
        final String err = SENTRY_COMM_FAILURE_MSG;
        LOGGER.error(err, e);
        throw new Exception(err, e);
      }
    }
    return sentryClient;
  }

  /*
   * Initialize HMS update object and assign its sequence number.
   * Call must be protected by notificationLock.
   */
  private PathsUpdate createHMSUpdate() {
    PathsUpdate update = new PathsUpdate(seqNum.incrementAndGet(), false);
    LOGGER.debug("#### Creating HMS Path Update SeqNum : [" + seqNum.get() + "]");
    return update;
  }

  /*
   * Get the last seen HMS path update sequence number from Sentry service.
   * Call must be protected by notificationLock.
   */
  private long getLastSeenHMSPathSeqNum() throws Exception {
    try {
      return getClient().getLastSeenHMSPathSeqNum();
    } catch (Exception e) {
      final String err = "Could not fetch the last seen HMS Path Sequence number from Sentry HDFS Service";
      LOGGER.error(err, e);
      resetClient();
      throw e;
    }
  }

  /*
   * Send update to Sentry service.
   * This method, when called from notifySentry(), is followed by updating lastSentSeqNumber.
   * When called directly, to send full updates (i.e. during initialization and from SyncTask),
   * the update sequence number does not change.
   * Call must be protected by notificationLock.
   */
  private void notifySentry_NoSeqNumIncr(PathsUpdate update) {
    final Timer.Context timerContext =
        SentryHdfsMetricsUtil.getNotifyHMSUpdateTimer.time();
    try {
      getClient().notifyHMSUpdate(update);
    } catch (Exception e) {
      final String err = "Could not send update to Sentry HDFS Service";
      LOGGER.error(err, e);
      resetClient();
      SentryHdfsMetricsUtil.getFailedNotifyHMSUpdateCounter.inc();
      throw new RuntimeException(err, e);
    } finally {
      timerContext.stop();
    }
  }

  /**
   * Send update to Sentry service and update last sent sequence number.
   * Called only if MetastorePlugin has been successfully initialized.
   * Call must be protected by notificationLock.
   */
  protected void notifySentry(PathsUpdate update) {
    try {
      notifySentry_NoSeqNumIncr(update);
    } finally {
      lastSentSeqNum = update.getSeqNum();
      LOGGER.debug("#### HMS Path Last update sent : ["+ lastSentSeqNum + "]");
    }
  }

  /*
   * Send full update to Sentry service.
   * Called only if MetastorePlugin has been successfully initialized.
   * Call must be protected by notificationLock.
   */
  private void notifySentryFullUpdate(long lastSent) {
    PathsUpdate fullImageUpdate = null;
    // access to authzPaths should be consistently protected by pathUpdateLock
    pathUpdateLock.readLock().lock();
    try {
      fullImageUpdate = authzPaths.createFullImageUpdate(lastSent);
    } finally {
      pathUpdateLock.readLock().unlock();
    }
    notifySentry_NoSeqNumIncr(fullImageUpdate);
    LOGGER.warn("#### Synced Sentry with update [" + lastSent + "]");
  }

  /*
   * When suspecting sentryClient comm error - reset the client
   * Call must be protected by notificationLock.
   */
  private void resetClient() {
    if (sentryClient != null) {
      try {
        sentryClient.close();
      } catch (Exception ignore) {
      }
      sentryClient = null;
    }
  }

  /**
   * Apply paths update to local cache.
   * Called only if MetastorePlugin has been successfully initialized.
   * Call must be protected by notificationLock.
   */
  protected void applyLocal(PathsUpdate update) {
    final Timer.Context timerContext =
        SentryHdfsMetricsUtil.getApplyLocalUpdateTimer.time();
    try {
      authzPaths.updatePartial(Lists.newArrayList(update), pathUpdateLock);
    } finally {
      timerContext.stop();
    }
    SentryHdfsMetricsUtil.getApplyLocalUpdateHistogram.update(
        update.getPathChanges().size());
  }

  /*
   * Apply paths update to local cache.
   * Send partial update to Sentry service.
   * Called only if MetastorePlugin has been successfully initialized.
   * Call must be protected by notificationLock.
   */
  private void updateLocalCacheAndNotifySentry(PathsUpdate update) {
    applyLocal(update);
    notifySentry(update);
  }

  /**
   * Apply paths update to local cache and send partial update to Sentry.
   * Called only if MetastorePlugin has been successfully initialized.
   * Call must be protected by notificationLock.
   */
  protected void processUpdate(PathsUpdate update) {
    updateLocalCacheAndNotifySentry(update);
  }

  /*
   * Check successfull initialization first, in each update callback method.
   * Null initError guarantees successful initialization.
   */
  private void assertInit() {
    if (initError != null) {
      throw new IllegalStateException(initErrorMsg, initError);
    }
  }

}
