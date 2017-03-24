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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.Timer;
import org.apache.sentry.hdfs.service.thrift.SentryHDFSService;
import org.apache.sentry.hdfs.service.thrift.TAuthzUpdateResponse;
import org.apache.sentry.hdfs.service.thrift.TPathsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPermissionsUpdate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentryHDFSServiceProcessor implements SentryHDFSService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryHDFSServiceProcessor.class);
  
  @Override
  public TAuthzUpdateResponse get_all_authz_updates_from(long permSeqNum, long pathSeqNum)
      throws TException {
    TAuthzUpdateResponse retVal = new TAuthzUpdateResponse();
    retVal.setAuthzPathUpdate(new LinkedList<TPathsUpdate>());
    retVal.setAuthzPermUpdate(new LinkedList<TPermissionsUpdate>());
    if (SentryPlugin.instance != null) {
      final Timer.Context timerContext =
          SentryHdfsMetricsUtil.getAllAuthzUpdatesTimer.time();
      try {
        List<PermissionsUpdate> permUpdates =
            SentryPlugin.instance.getAllPermsUpdatesFrom(permSeqNum);
        SentryHdfsMetricsUtil.getPermUpdateHistogram.update(permUpdates.size());
        List<PathsUpdate> pathUpdates =
            SentryPlugin.instance.getAllPathsUpdatesFrom(pathSeqNum);
        SentryHdfsMetricsUtil.getPathUpdateHistogram.update(pathUpdates.size());
        for (PathsUpdate update : pathUpdates) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("### Sending PATH preUpdate seq [" + update.getSeqNum() + "] ###");
            LOGGER.debug("### Sending PATH preUpdate [" + update.toThrift() + "] ###");
          }
          retVal.getAuthzPathUpdate().add(update.toThrift());
        }
        for (PermissionsUpdate update : permUpdates) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("### Sending PERM preUpdate seq [" + update.getSeqNum() + "] ###");
            LOGGER.debug("### Sending PERM preUpdate [" + update.toThrift() + "] ###");
          }
          retVal.getAuthzPermUpdate().add(update.toThrift());
        }
        if (LOGGER.isDebugEnabled()) {
          StringBuilder permSeq = new StringBuilder("<");
          for (PermissionsUpdate permUpdate : permUpdates) {
            permSeq.append(permUpdate.getSeqNum()).append(",");
          }
          permSeq.append(">");
          StringBuilder pathSeq = new StringBuilder("<");
          for (PathsUpdate pathUpdate : pathUpdates) {
            pathSeq.append(pathUpdate.getSeqNum()).append(",");
          }
          pathSeq.append(">");
          LOGGER.debug("#### Updates requested from HDFS ["
              + "permReq=" + permSeqNum + ", permResp=" + permSeq + "] "
              + "[pathReq=" + pathSeqNum + ", pathResp=" + pathSeq + "]");
        }
      } catch (Exception e) {
        LOGGER.error("Error Sending updates to downstream Cache", e);
        throw new TException(e);
      } finally {
        timerContext.stop();
      }
    } else {
      LOGGER.error("SentryPlugin not initialized yet !!");
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
