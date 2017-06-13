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

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.hdfs.SentryAuthzUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SentryUpdater {

  private SentryHDFSServiceClient sentryClient;
  private final Configuration conf;
  private final SentryAuthorizationInfo authzInfo;

  private static Logger LOG = LoggerFactory.getLogger(SentryUpdater.class);

  SentryUpdater(Configuration conf, SentryAuthorizationInfo authzInfo) throws Exception {
    this.conf = conf;
    this.authzInfo = authzInfo;
  }

  SentryAuthzUpdate getUpdates() {
    if (sentryClient == null) {
      try {
        sentryClient = SentryHDFSServiceClientFactory.create(conf);
      } catch (Exception e) {
        LOG.error("Error connecting to Sentry ['{}'] !!",
            e.getMessage());
        sentryClient = null;
        return null;
      }
    }
    try {
      SentryAuthzUpdate sentryUpdates = sentryClient.getAllUpdatesFrom(
          authzInfo.getAuthzPermissions().getLastUpdatedSeqNum() + 1,
          authzInfo.getAuthzPaths().getLastUpdatedSeqNum() + 1);
      return sentryUpdates;
    } catch (Exception e)  {
      sentryClient = null;
      LOG.error("Error receiving updates from Sentry !!", e);
      return null;
    }
  }

}
