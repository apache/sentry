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
import java.util.Map;

import sentry.com.codahale.metrics.Timer.Context;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.sentry.hdfs.service.thrift.SentryHDFSService;
import org.apache.sentry.hdfs.service.thrift.TAuthzUpdateRequest;
import org.apache.sentry.hdfs.service.thrift.TAuthzUpdateResponse;
import org.apache.sentry.hdfs.service.thrift.TPathsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPermissionsUpdate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process requests from HDFS Name Node plugin.
 * The only supported request is {@link #get_all_authz_updates_from(long, long)}.
 */
public class SentryHDFSServiceProcessor implements SentryHDFSService.Iface {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryHDFSServiceProcessor.class);

  // Flag to prevent that multiple paths updates are sent to HDFS NameNodes at the same time.
  // This helps to reduce memory consumption on large path images.
  private static final AtomicBoolean pathsRetrieverBusy = new AtomicBoolean(false);

  @Override
  public TAuthzUpdateResponse get_all_authz_updates_from(long permSeqNum, long pathSeqNum) throws TException {
   throw new UnsupportedOperationException(
       "get_all_authz_updates_from() is not supported due to known bugs. Use get_authz_updates() instead.");
  }

  @Override
  public TAuthzUpdateResponse get_authz_updates(TAuthzUpdateRequest request)
      throws TException {
    TAuthzUpdateResponse retVal = new TAuthzUpdateResponse();

    if (SentryPlugin.instance == null) {
      LOGGER.error("SentryPlugin not initialized yet !!");
      retVal.setAuthzPathUpdate(Collections.<TPathsUpdate>emptyList());
      retVal.setAuthzPermUpdate(Collections.<TPermissionsUpdate>emptyList());
      return retVal;
    }

    try (Context timerContext = SentryHdfsMetricsUtil.getAllAuthzUpdatesTimer.time()) {
      retVal.setAuthzPermUpdate(getPermissionsUpdatesFrom(request));
      retVal.setAuthzPathUpdate(getPathsUpdatesFrom(request));
    } catch (Exception e) {
      LOGGER.error("Error Sending updates to downstream Cache", e);
      throw new TException(e);
    }

    return retVal;
  }

  /**
   * Retrieves and returns new requested permissions updates.
   *
   * @param request The requested thrift object that contains the permission seqNum
   * @return A list of delta updates or a full image retrieved. It returns an empty list if there
   *         are not updates.
   * @throws Exception If an error occurred while retrieving the updates.
   */
  private List<TPermissionsUpdate> getPermissionsUpdatesFrom(TAuthzUpdateRequest request)
      throws Exception {

    LOGGER.debug("PERMISSIONS updates requested from HDFS [SeqNum={}]",
        request.getPermSeqNum());

    List<PermissionsUpdate> permUpdates =
        SentryPlugin.instance.getAllPermsUpdatesFrom(request.getPermSeqNum());

    List<TPermissionsUpdate> retPermUpdates = new ArrayList<>(permUpdates.size());
    for (PermissionsUpdate update : permUpdates) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Sending PERM preUpdate seq [{}], [{}]",
            update.getSeqNum(), update.toThrift());
      }

      retPermUpdates.add(update.toThrift());
    }

    SentryHdfsMetricsUtil.getPermUpdateHistogram.update(permUpdates.size());
    return retPermUpdates;
  }

  /**
   * Retrieves and returns new requested paths updates. To avoid too much memory consumption
   * with large full images, then this method will serialize the access to the requested updates
   * to only one request at a time.
   *
   * @param request The requested thrift object that contains the paths seqNum and imgNum
   * @return A list of delta updates or a full image retrieved. It returns an empty list if there
   *         are not updates or another request is happening at the same time.
   * @throws Exception If an error occurred while retrieving the updates.
   */
  private List<TPathsUpdate> getPathsUpdatesFrom(TAuthzUpdateRequest request) throws Exception {
    LOGGER.debug("PATHS updates requested from HDFS [ImgNum={}, SeqNum={}]",
        request.getPathImgNum(), request.getPathSeqNum());

    // If another paths updates request is in progress by another HDFS NameNode, then we
    // return an empty image for now to avoid a large memory consumption
    if (!pathsRetrieverBusy.compareAndSet(false, true)) {
      LOGGER.debug("PATHS updates are not available because another request is in progress.");
      return Collections.emptyList();
    }

    try {
      List<PathsUpdate> pathUpdates =
          SentryPlugin.instance.
              getAllPathsUpdatesFrom(request.getPathSeqNum(), request.getPathImgNum());

      List<TPathsUpdate> retPathUpdates = new ArrayList<>(pathUpdates.size());
      for (PathsUpdate update : pathUpdates) {
        LOGGER.debug("Sending PATH preUpdate seq [{}], [{}]",
            update.getSeqNum(), update.getImgNum());
        retPathUpdates.add(update.toThrift());
      }

      SentryHdfsMetricsUtil.getPathUpdateHistogram.update(pathUpdates.size());
      return retPathUpdates;
    } catch (Exception e) {
      throw e;
    } finally {
      pathsRetrieverBusy.set(false);
    }
  }

  @Override
  public void handle_hms_notification(TPathsUpdate update) throws TException {
    throw new UnsupportedOperationException("handle_hms_notification");
  }

  @Override
  public long check_hms_seq_num(long pathSeqNum) throws TException {
    throw new UnsupportedOperationException("check_hms_seq_num");
  }

  /**
   * Not implemented for the time being..
   */
  @Override
  public Map<String, List<String>> get_all_related_paths(String arg0,
      boolean arg1) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

}
