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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.sentry.provider.db.SentryMetastoreListenerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class MetastorePlugin extends SentryMetastoreListenerPlugin {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(MetastorePlugin.class);
  
  private final Configuration conf;
  private SentryHDFSServiceClient sentryClient;

  //Initialized to some value > 1 so that the first update notification
 // will trigger a full Image fetch
  private final AtomicInteger seqNum = new AtomicInteger(5);

  public MetastorePlugin(Configuration conf) {
    this.conf = conf;
    try {
      sentryClient = new SentryHDFSServiceClient(conf);
    } catch (IOException e) {
      sentryClient = null;
      LOGGER.error("Could not connect to Sentry HDFS Service !!", e);
    }
  }

  @Override
  public void addPath(String authzObj, String path) {
    PathsUpdate update = createHMSUpdate();
    update.newPathChange(authzObj).addToAddPaths(PathsUpdate.cleanPath(path));
    try {
      notifySentry(update);
    } catch (MetaException e) {
      LOGGER.error("Could not send update to Sentry HDFS Service !!", e);
    }
  }

  @Override
  public void removeAllPaths(String authzObj) {
    PathsUpdate update = createHMSUpdate();
    update.newPathChange(authzObj).addToDelPaths(Lists.newArrayList(PathsUpdate.ALL_PATHS));
    try {
      notifySentry(update);
    } catch (MetaException e) {
      LOGGER.error("Could not send update to Sentry HDFS Service !!", e);
    }
  }

  @Override
  public void removePath(String authzObj, String path) {
    PathsUpdate update = createHMSUpdate();
    update.newPathChange(authzObj).addToDelPaths(PathsUpdate.cleanPath(path));
    try {
      notifySentry(update);
    } catch (MetaException e) {
      LOGGER.error("Could not send update to Sentry HDFS Service !!", e);
    }
  }

  private SentryHDFSServiceClient getClient() {
    if (sentryClient == null) {
      try {
        sentryClient = new SentryHDFSServiceClient(conf);
      } catch (IOException e) {
        sentryClient = null;
        LOGGER.error("Could not connect to Sentry HDFS Service !!", e);
      }
    }
    return sentryClient;
  }

  private PathsUpdate createHMSUpdate() {
    PathsUpdate update = new PathsUpdate(seqNum.incrementAndGet(), false);
    return update;
  }

  private void notifySentry(PathsUpdate update) throws MetaException {
    try {
      getClient().notifyHMSUpdate(update);
    } catch (IOException e) {
      throw new MetaException("Error sending update to Sentry [" + e.getMessage() + "]");
    }
  }

}
