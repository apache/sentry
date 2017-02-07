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
 *    <li> Starts housekeeping thread to run periodically SyncTask - see below what it does.
 *   </ul>
 *  </li>
 *  <li> Upon receiving path update notification from the hosting client code, via addPath(),
 *       removePath(), removeAllPaths(), and renameAuthzObject() callback methods:
 *   <ul>
 *    <li> Updates local HMS cache accordingly.
 *    <li> Sends partial update with the assigned sequence number to the Sentry daemon.
 *    <li> Special case - skip sending partial update to Sentry if the initial full update from
 *         housekeeping thread is still in progress (firstSync == false). Still update the local cache.
 *    <li> Maintains the latest Sentry partial update sequence number, incrementing it by 1 on each update.
 *   </ul>
 *  </li>
 *  <li> Periodically, from the housekeeping thread:
 *   <ul>
 *    <li> Contacts the Sentry daemon to ask for the sequence number of the latest received update.
 *    <li> If the sequence number returned by the Sentry daemon does not match the sequence number of the
 *         latest update sent from MetastorePlugin, send the full HMS paths image to the Sentry daemon.
 *    <li> After the very first successful full HMS paths update to Sentry, set firstSync to true.
 *         to signal the rest of the code that from this moment all partial updates should be sent to Sentry,
 *         in addition to updating the local cache.
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
 * MetastorePlugin initialization (object construction) failure reasons:
 * <ul>
 *  <li> HMS cache cannot be initialized, usually due to some invalid HMS path entries.
 * </ul>
 *
 * <p>
 * MetastorePlugin is still constructed, in consideration with the design of the existing client code.
 * However, such an instance is marked as invalid; all update APIs throw IllegalStateException with
 * the appropriate error message and root cause exception.
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
 * with sequential update number. This is achieved by using notificationLock over two operations:
 * assigning sequence number to the update, and sending this update to Sentry. The same lock is used
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
  /*
   * The firstSync value, set initially to false, is set to true by SyncTask from
   * housekeeping thread, immediately after pushing the first successsful full update to Sentry.
   * Prior to firstSync == true, all partial updates happen only in the local cache.
   * This code ensures that access to firstSync is protected by notificationLock
   */
  protected boolean firstSync;

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
   * This task is scheduled to run periodically, to make sure Sentry (SentryPlugin
   * class to be exact) has all the latest HMS path updates.
   *
   * It compares the last sequence number of a partial update sent to Sentry
   * against the sequence number of last update as reported by Sentry.
   * If the two are different, full update is triggered.
   *
   * The initial full update must reach Sentry before any partial updates.
   * Since any full update would also include the all the subsequent partial
   * updates as well anyway, sending partial updates before the first full
   * update would be redundant. It would also require more care on SentryPlugin
   * side to handle updates with potentially duplicate sequence numbers.
   *
   * The firstSync variable initialized to false in the constructor,
   * will be set to true in the run() method on the first successfull full
   * update. Prior to that event, all partial updates will only be commited
   * to the local memory cache.
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
        /*
         * Most common branch, after the first full update (firstSync == true).
         * Do full update only if out-of-sync is detected.
         */ 
        if (firstSync) {
          // Out-of-sync! Normally should not happen, so worth logging as a warning.
          if (lastSeenBySentry != lastSent) {
            LOGGER.warn("#### Sentry not in sync with HMS [" + lastSeenBySentry + ", "
              + lastSent + "]");
            notifySentryFullUpdate(lastSent);
          } // else we are ok, which is common, so we don't want to log anything
        /*
         * Less common branch - only before the first successful full update.
         * Do full update regardless of the sequence numbers on both sides.
         * Don't want to depend on how specifically SentryPlugin and MetastorePlugin
         * logic initialize their sequence numbers to guarantee that they are
         * initially different. First full update is always mandatory.
         *
         * TODO: if at least MetastorePlugin implements persistent incrementing
         * sequence numbers, this logic can be optimized to avoid the initial fill
         * update. It can optimize the situation when MetastorePlugin is restarted
         * while Sentry keeps running and allready has the latest updates.
         */
        } else { // firstSync == false
          // still print both sequence numbers out of curiosity, to see how they get initialized
          LOGGER.info("#### Trying to send first full update to Sentry [" + lastSeenBySentry + ", "
            + lastSent + "]");
          notifySentryFullUpdate(lastSent);
          LOGGER.info("#### First successful full update with Sentry");
          /*
           * If initial full update succeeded, set firstSync to true to never do unconditional
           * full update ever again. Also, firstSync == true signals to the rest of the code
           * that from this point on partial updates will be sent to Sentry, not only update
           * the local cache.
           * If notifySentryFullUpdate() fails, firstSync remains false, so another full
           * update will be attempted.
           */
          firstSync = true;
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

    /* Initialization Step #2: set the state to perform sync with Sentry.
     * Kerberos may be initialized only AFTER constructing MetastorePlugin,
     * so the initial full update would be impossible at this point.
     * We rely on SyncTask to eventually establish connection and
     * push the first full update to Sentry, signaled by setting firstSync to true.
     */
    notificationLock.lock();
    try {
      this.lastSentSeqNum = seqNum.get();
      this.firstSync = false;
    } finally {
      notificationLock.unlock();
    }
    this.initError = null;
    this.initErrorMsg = null;

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
      /* Send updates to Sentry only after the first full update from SyncTask
       * Note, that full updates from SyncTask happen by calling notifySentry_NoSeqNumIncr()
       * directly, via notifySentryFullUpdate(), so firstSync value is not consulted.
       */
      if (firstSync) {
        notifySentry_NoSeqNumIncr(update);
      } else {
        LOGGER.warn("#### Caching partial Sentry update " + update.getSeqNum() + "; initial full update still in progress");
      }
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
