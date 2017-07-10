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

import com.codahale.metrics.Timer.Context;
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

    try (Context timerContext =
                 SentryHdfsMetricsUtil.getAllAuthzUpdatesTimer.time()){
      List<PermissionsUpdate> permUpdates =
          SentryPlugin.instance.getAllPermsUpdatesFrom(request.getPermSeqNum());
      SentryHdfsMetricsUtil.getPermUpdateHistogram.update(permUpdates.size());
      List<PathsUpdate> pathUpdates =
          SentryPlugin.instance.getAllPathsUpdatesFrom(request.getPathSeqNum(), request.getPathImgNum());
      SentryHdfsMetricsUtil.getPathUpdateHistogram.update(pathUpdates.size());

      List<TPathsUpdate> retPathUpdates = new ArrayList<>(pathUpdates.size());
      for (PathsUpdate update : pathUpdates) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Sending PATH preUpdate seq [{}], [{}]",
              update.getSeqNum(), update.getImgNum());
        }
        retPathUpdates.add(update.toThrift());
      }
      retVal.setAuthzPathUpdate(retPathUpdates);

      List<TPermissionsUpdate>retPermUpdates = new ArrayList<>(permUpdates.size());
      for (PermissionsUpdate update : permUpdates) {
        LOGGER.debug("Sending PERM preUpdate seq [{}], [{}]",
                update.getSeqNum(), update.toThrift());
        retPermUpdates.add(update.toThrift());
      }
      retVal.setAuthzPermUpdate(retPermUpdates);

      if (LOGGER.isDebugEnabled()) {
        StringBuilder permSeq = new StringBuilder("<");
        for (PermissionsUpdate permUpdate : permUpdates) {
          permSeq.append(permUpdate.getSeqNum()).append(",");
        }
        permSeq.append(">");
        StringBuilder pathSeq = new StringBuilder("<");
        for (PathsUpdate pathUpdate : pathUpdates) {
          pathSeq.append(pathUpdate.getSeqNum()).append(",");
          pathSeq.append(pathUpdate.getImgNum()).append(",");
        }
        pathSeq.append(">");
        LOGGER.debug("Updates requested from HDFS [permReq={}, permResp={}] [pathReq={}, pathResp={}]",
            new Object[]{request.getPermSeqNum(), permSeq, request.getPathSeqNum(), pathSeq});
      }
    } catch (Exception e) {
      LOGGER.error("Error Sending updates to downstream Cache", e);
      throw new TException(e);
    }

    return retVal;
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
